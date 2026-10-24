package com.babelqueue.redis;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import io.lettuce.core.RedisCommandInterruptedException;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.IntToDoubleFunction;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Reserves messages from a Redis queue, decodes and validates each, routes it to the handler
 * registered for its URN, and acks it on success. Two layouts are supported ({@link RedisLayout}):
 *
 * <ul>
 *   <li><b>{@code LEGACY}</b> (default) &mdash; the Java-owned reliable queue. A Lua script atomically
 *       {@code LMOVE}s the head of {@code <queue>} to {@code <queue>:processing} and records a deadline
 *       member {@code <token>:<raw>} in {@code <queue>:processing:deadlines}. A successful handler acks
 *       with a gated script (the deadline member must still exist, so a late ack by a worker whose
 *       reservation was already swept touches nothing). Before every reserve the consumer <em>sweeps</em>
 *       expired deadline members back to the ready list, so a message held by a killed worker is
 *       redelivered after {@code retry_after} (default 90 s) with its body unchanged. The ready list is
 *       polled non-blockingly with a 100 ms &rarr; 1 s backoff.</li>
 *   <li><b>{@code ZSET}</b> (opt-in) &mdash; Laravel's layout on a shared queue ({@code queues:<queue>},
 *       {@code :delayed}, {@code :reserved}, {@code :notify}). Reserve is Laravel's pop script: it
 *       increments {@code attempts} and records the re-encoded job in {@code :reserved} with a deadline;
 *       the handler still sees the job exactly as stored, with {@code attempts} reported as the stored
 *       value + 1. Due {@code :delayed} and expired {@code :reserved} members are migrated back to the
 *       ready list before every reserve (an unacknowledged message is redelivered with one more attempt).
 *       A body the script cannot decode is dead-lettered to {@code <queue>.dlq}.</li>
 * </ul>
 *
 * <p>The poll loop never stops on a bad message &mdash; observe via {@code onError}/{@code onUnknownUrn}.
 * A non-conformant envelope is reported and moved to {@code <queue>.dlq}.
 *
 * <p>A throwing handler is reported, then:
 * <ul>
 *   <li><b>zset layout</b> &mdash; the message is released to {@code :delayed} with {@code attempts + 1}
 *       (A2) once a delay source is configured. The delay is, in order of precedence: the explicit {@code failureDelaySeconds} (a constant);
 *       else the retry backoff {@code d(n) = min(max, base * multiplier^(n-1))} with {@code n} the 1-based
 *       delivery number the handler saw (ADR-0034 section 9, see {@code retryBackoff*}). When neither is
 *       configured the reservation is left alone and the message is redelivered once {@code retry_after}
 *       expires, exactly as in the legacy layout (ADR-0034 section 9; .NET {@code Leave} parity) &mdash;
 *       this module has no max-attempts, so there is no hot redelivery loop by default. A resulting delay
 *       is capped at {@link #MAX_FAILURE_DELAY_SECONDS}.</li>
 *   <li><b>legacy layout</b> &mdash; the reservation is left to expire and be recovered after
 *       {@code retry_after} (at-least-once); no backoff or failure delay applies.</li>
 * </ul>
 *
 * <p>A legacy reserved value carrying out-of-band headers (e.g. a W3C {@code traceparent}, ADR-0028) is
 * wrapped in a transport-owned {@link RedisFrame}; the consumer unframes it transparently and surfaces
 * the headers to a {@link HeaderHandler}. The zset layout carries no frames or headers: Laravel's
 * script re-encodes the job with cjson, so there is no out-of-band channel on a shared queue (A3) &mdash;
 * the {@code trace_id} still travels in the envelope.
 */
public final class RedisConsumer {

    /** Notified of a non-conformant envelope, an unmapped URN (no {@code onUnknownUrn}), or a throwing handler. */
    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error, Envelope envelope, String body);
    }

    /** Called instead of erroring when a URN has no handler; the message is then acked. */
    @FunctionalInterface
    public interface UnknownUrnHandler {
        void onUnknownUrn(Envelope envelope, String body);
    }

    /**
     * A header-aware handler: like {@link BabelHandler} but also receives the message's
     * out-of-band transport headers (e.g. a carried {@code traceparent}; empty when none).
     * It is the consume-side seam for the optional core
     * {@code com.babelqueue.otel.Tracing#wrapHandler(io.opentelemetry.api.trace.Tracer,
     * com.babelqueue.idempotency.Handler, java.util.function.Supplier)}:
     *
     * <pre>{@code
     * RedisConsumer.builder(redis, "orders")
     *     .handler(urn, (env, body, headers) -> Tracing
     *         .wrapHandler(tracer, h, () -> headers)
     *         .handle(env))
     *     .build();
     * }</pre>
     */
    @FunctionalInterface
    public interface HeaderHandler {
        void handle(Envelope envelope, String body, Map<String, String> headers) throws Exception;
    }

    /** Internal adapter so a {@link BabelHandler} and a {@link HeaderHandler} share one dispatch path. */
    @FunctionalInterface
    private interface Registered {
        void handle(Envelope envelope, String body, Map<String, String> headers) throws Exception;
    }

    /**
     * Upper bound on a zset failure delay, in seconds (30 days). A larger or infinite delay (a "park it"
     * policy) is clamped here so the {@code :delayed} score can never overflow into an instant redelivery.
     */
    public static final double MAX_FAILURE_DELAY_SECONDS = 30d * 24 * 3600;

    private static final long BACKOFF_START_MS = 100;
    private static final long BACKOFF_CAP_MS = 1000;

    private final RedisQueueOps ops;
    private final String queue;
    private final Map<String, Registered> handlers;
    private final long blockTimeoutSeconds;
    private final Double failureDelaySeconds;
    private final IntToDoubleFunction retryDelay;
    private final ErrorHandler onError;
    private final UnknownUrnHandler onUnknownUrn;
    private final LongSupplier nanoClock;
    private final LongConsumer sleeper;
    private long backoffMs = BACKOFF_START_MS;

    private RedisConsumer(Builder builder) {
        this.queue = builder.queue;
        this.ops = builder.ops != null
            ? builder.ops
            : new RedisQueueOps(
                builder.redis,
                builder.queue,
                builder.layout,
                builder.keyPrefix,
                builder.retryAfterSeconds,
                builder.migrationBatchSize,
                builder.clock);
        this.handlers = Map.copyOf(builder.handlers);
        this.blockTimeoutSeconds = builder.blockTimeoutSeconds;
        this.failureDelaySeconds = builder.failureDelaySeconds;
        this.retryDelay = builder.resolveRetryDelay();
        this.onError = builder.onError;
        this.onUnknownUrn = builder.onUnknownUrn;
        this.nanoClock = builder.nanoClock;
        this.sleeper = builder.sleeper;
    }

    public static Builder builder(RedisCommands<String, String> redis, String queue) {
        return new Builder(redis, queue);
    }

    /**
     * Reserve at most one message, route it, and ack it when handled. Waits up to the configured
     * block timeout (in slices of at most one second, so a due delayed or expired reservation is noticed
     * promptly) for a message to arrive. Returns the number of messages reserved this call (0 or 1).
     */
    public int poll() {
        long startNanos = nanoClock.getAsLong();
        long timeoutNanos = blockTimeoutSeconds * 1_000_000_000L;
        while (true) {
            Reservation reservation = ops.reserve();
            if (reservation != null) {
                backoffMs = BACKOFF_START_MS;
                handle(reservation);
                return 1;
            }
            long remainingNanos = blockTimeoutSeconds <= 0
                ? Long.MAX_VALUE
                : timeoutNanos - (nanoClock.getAsLong() - startNanos);
            if (remainingNanos <= 0 || Thread.currentThread().isInterrupted()) {
                return 0;
            }
            try {
                waitForWork(remainingNanos);
            } catch (RedisCommandInterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return 0;
            }
        }
    }

    /** Wait one slice (&le; 1 s, &le; the remaining budget): a notify block (zset) or a backoff sleep (legacy). */
    private void waitForWork(long remainingNanos) {
        if (ops.layout() == RedisLayout.ZSET) {
            double slice = Math.min(1.0, remainingNanos / 1e9);
            ops.awaitNotify(slice);
            return;
        }
        long sliceMs = Math.max(1, Math.min(backoffMs, remainingNanos / 1_000_000L));
        sleeper.accept(sliceMs);
        backoffMs = Math.min(backoffMs * 2, BACKOFF_CAP_MS);
    }

    /** Poll until the current thread is interrupted (each poll waits, so this does not busy-loop). */
    public void run() {
        run(() -> !Thread.currentThread().isInterrupted());
    }

    /** Poll while {@code shouldContinue} returns true. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            poll();
        }
    }

    private void handle(Reservation reservation) {
        boolean zset = ops.layout() == RedisLayout.ZSET;
        // Legacy: the stored value may be a transport-owned header frame (ADR-0028) or a bare envelope.
        // Zset: Laravel's layout has no frame (A3) - the value is the job as stored.
        RedisFrame.Unframed unframed = zset
            ? new RedisFrame.Unframed(reservation.body(), Map.of())
            : RedisFrame.unframe(reservation.body());
        String body = unframed.body();
        Map<String, String> headers = unframed.headers();
        Envelope envelope = EnvelopeCodec.decode(body);

        if (reservation.poison()) {
            report(new BabelQueueException("Dead-lettered a Redis job the reserve script could not decode."),
                envelope, body);
            ops.deadLetter(reservation);
            return;
        }
        if (!EnvelopeCodec.accepts(envelope)) {
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Redis."),
                envelope, body);
            ops.deadLetter(reservation);
            return;
        }
        if (zset) {
            // Laravel's reserve incremented attempts in the stored copy; hand the handler stored + 1.
            envelope = envelope.withAttempts(Math.max(envelope.attempts(), 0) + 1);
        }

        String urn = EnvelopeCodec.urn(envelope);
        Registered handler = handlers.get(urn);
        if (handler == null) {
            if (onUnknownUrn != null) {
                onUnknownUrn.onUnknownUrn(envelope, body);
                ops.ack(reservation);
            } else {
                // Leave the reservation to expire and be recovered.
                report(new UnknownUrnException(urn), envelope, body);
            }
            return;
        }

        try {
            handler.handle(envelope, body, headers);
        } catch (Exception error) {
            report(error, envelope, body);
            if (zset) {
                Double delay = failureDelay(envelope);
                if (delay != null) {
                    ops.release(reservation, delay);
                }
                // else: no failureDelaySeconds / backoff configured -> leave the reservation, recovered after retry_after.
            }
            // Legacy: leave the reservation, at-least-once, recovered after retry_after.
            return;
        }
        ops.ack(reservation);
    }

    /**
     * Zset failure delay in seconds: the explicit {@code failureDelaySeconds} wins; else the backoff for
     * {@code n} = the delivery number the handler saw ({@code envelope.attempts()} is already stored + 1);
     * else {@code null} (nothing configured: the caller leaves the reservation to expire). A {@code NaN} or
     * negative policy result counts as 0; {@code +Infinity} or anything above
     * {@link #MAX_FAILURE_DELAY_SECONDS} is clamped to it.
     */
    private Double failureDelay(Envelope envelope) {
        if (failureDelaySeconds != null) {
            return clampDelay(failureDelaySeconds);
        }
        if (retryDelay == null) {
            return null;
        }
        return clampDelay(retryDelay.applyAsDouble(Math.max(envelope.attempts(), 1)));
    }

    private static double clampDelay(double delay) {
        if (Double.isNaN(delay) || delay < 0) {
            return 0.0;
        }
        return Math.min(delay, MAX_FAILURE_DELAY_SECONDS);
    }

    private void report(Throwable error, Envelope envelope, String body) {
        if (onError != null) {
            onError.onError(error, envelope, body);
        }
    }

    /** The logical queue name this consumer reserves from. */
    public String queue() {
        return queue;
    }

    /**
     * The in-flight key: {@code <queue>:processing} (legacy list) or {@code <prefix><queue>:reserved}
     * (zset sorted set).
     */
    public String processingQueue() {
        return ops.inFlightKey();
    }

    /** Fluent builder for {@link RedisConsumer}. */
    public static final class Builder {
        private final RedisCommands<String, String> redis;
        private final String queue;
        private final Map<String, Registered> handlers = new HashMap<>();
        private long blockTimeoutSeconds = 5;
        private RedisLayout layout = RedisLayout.LEGACY;
        private String keyPrefix;
        private Long retryAfterSeconds = RedisQueueOps.DEFAULT_RETRY_AFTER_SECONDS;
        private long migrationBatchSize = -1;
        private Double failureDelaySeconds;
        private IntToDoubleFunction retryPolicy;
        private double retryBackoffSeconds;
        private double retryBackoffMultiplier = 1.0;
        private double retryBackoffMaxSeconds;
        private boolean retryJitterFull;
        private DoubleSupplier random = () -> java.util.concurrent.ThreadLocalRandom.current().nextDouble();
        private ErrorHandler onError;
        private UnknownUrnHandler onUnknownUrn;
        private RedisQueueOps ops;
        private DoubleSupplier clock = () -> System.currentTimeMillis() / 1000.0;
        private LongSupplier nanoClock = System::nanoTime;
        private LongConsumer sleeper = Builder::sleepMillis;

        private Builder(RedisCommands<String, String> redis, String queue) {
            this.redis = Objects.requireNonNull(redis, "redis");
            this.queue = Objects.requireNonNull(queue, "queue");
        }

        private static void sleepMillis(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /** Register {@code handler} for {@code urn} (the last registration wins). */
        public Builder handler(String urn, BabelHandler handler) {
            this.handlers.put(urn, (env, body, headers) -> handler.handle(env, body));
            return this;
        }

        /**
         * Register a header-aware {@code handler} for {@code urn} (the last registration
         * wins) &mdash; it additionally receives the message's out-of-band transport headers,
         * the seam for OpenTelemetry {@code traceparent} propagation (ADR-0028). Legacy layout only;
         * the zset layout carries no headers (A3), so the map is empty there.
         */
        public Builder handler(String urn, HeaderHandler handler) {
            this.handlers.put(urn, handler::handle);
            return this;
        }

        public Builder handlers(Map<String, BabelHandler> handlers) {
            handlers.forEach((urn, h) -> this.handlers.put(urn, (env, body, headers) -> h.handle(env, body)));
            return this;
        }

        /**
         * Total seconds one {@link #poll()} waits for a message (default 5; {@code 0} waits indefinitely).
         * The wait is sliced into blocks of at most one second.
         */
        public Builder blockTimeoutSeconds(long seconds) {
            this.blockTimeoutSeconds = seconds;
            return this;
        }

        /** The key layout: {@link RedisLayout#LEGACY} (default) or {@link RedisLayout#ZSET} (Laravel). */
        public Builder layout(RedisLayout layout) {
            this.layout = Objects.requireNonNull(layout, "layout");
            return this;
        }

        /** Zset layout: the key prefix (default {@code queues:}, Laravel's default). */
        public Builder keyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
            return this;
        }

        /**
         * Seconds before an unacknowledged reservation is recovered (default 90). Must not be negative;
         * see {@link #retryAfterDisabled()} to turn recovery off.
         */
        public Builder retryAfterSeconds(long seconds) {
            this.retryAfterSeconds = seconds;
            return this;
        }

        /**
         * Disable recovery of this consumer's own reservations: their deadline is {@code +inf}, so they stay
         * reserved until acked. (The legacy sweep of <em>other</em> workers' expired reservations still runs.)
         */
        public Builder retryAfterDisabled() {
            this.retryAfterSeconds = null;
            return this;
        }

        /** Max items moved per migrate / sweep call (default {@code -1} = unbounded; otherwise positive). */
        public Builder migrationBatchSize(long batchSize) {
            this.migrationBatchSize = batchSize;
            return this;
        }

        /**
         * Zset layout: when a handler throws, release the message to {@code :delayed} after a constant
         * {@code seconds} (with {@code attempts + 1}). Unset by default; when set it takes precedence over
         * every {@code retryBackoff*} setting. When neither is set the reservation is left to expire and the
         * message is redelivered after {@code retry_after}. Must be finite and {@code >= 0}; values above
         * {@link RedisConsumer#MAX_FAILURE_DELAY_SECONDS} are clamped to it.
         */
        public Builder failureDelaySeconds(double seconds) {
            if (!(seconds >= 0) || Double.isInfinite(seconds)) {
                throw new IllegalArgumentException("failureDelaySeconds must be a finite number >= 0");
            }
            this.failureDelaySeconds = seconds;
            return this;
        }

        /**
         * Zset layout: a custom retry backoff policy, {@code delayForAttempt(n)} = seconds to wait before
         * redelivery, with {@code n} the 1-based delivery number that just failed. It is shaped to take the
         * core {@code RetryBackoff} (ADR-0034 section 9) as {@code policy::delaySeconds}; this module's core
         * pin (1.11.0) predates that class, so the type itself is not referenced here. Overrides the
         * {@code retryBackoff*} scalar knobs; an explicit {@link #failureDelaySeconds} still wins over both.
         */
        public Builder retryBackoff(IntToDoubleFunction delayForAttempt) {
            this.retryPolicy = Objects.requireNonNull(delayForAttempt, "delayForAttempt");
            return this;
        }

        /**
         * A9 {@code retry_backoff}: the base delay in seconds; zset layout only. Unset (or {@code 0}) the
         * scalar backoff is off and a failing message keeps its reservation until {@code retry_after};
         * a positive base turns the backoff on (use {@link #failureDelaySeconds} {@code (0)} to release
         * immediately).
         */
        public Builder retryBackoff(double seconds) {
            if (!(seconds >= 0) || Double.isInfinite(seconds)) {
                throw new IllegalArgumentException("retry_backoff must be a finite number >= 0");
            }
            this.retryBackoffSeconds = seconds;
            return this;
        }

        /** A9 {@code retry_backoff_multiplier} (default {@code 1}, must be {@code >= 1}). */
        public Builder retryBackoffMultiplier(double multiplier) {
            if (!(multiplier >= 1) || Double.isInfinite(multiplier)) {
                throw new IllegalArgumentException("retry_backoff_multiplier must be a finite number >= 1");
            }
            this.retryBackoffMultiplier = multiplier;
            return this;
        }

        /** A9 {@code retry_backoff_max} in seconds (default {@code 0} = unlimited). */
        public Builder retryBackoffMax(double seconds) {
            if (!(seconds >= 0) || Double.isInfinite(seconds)) {
                throw new IllegalArgumentException("retry_backoff_max must be a finite number >= 0");
            }
            this.retryBackoffMaxSeconds = seconds;
            return this;
        }

        /** A9 {@code retry_jitter}: {@code "none"} (default) or {@code "full"} ({@code uniform(0, d(n))}). */
        public Builder retryJitter(String jitter) {
            Objects.requireNonNull(jitter, "jitter");
            // Same normalisation as the core RetryBackoff.Jitter.parse (strip + case-insensitive).
            switch (jitter.strip().toLowerCase(java.util.Locale.ROOT)) {
                case "none" -> this.retryJitterFull = false;
                case "full" -> this.retryJitterFull = true;
                default -> throw new IllegalArgumentException("retry_jitter must be 'none' or 'full', got: " + jitter);
            }
            return this;
        }

        /** Test seam: the uniform [0,1) source used by full jitter. */
        Builder randomSource(DoubleSupplier source) {
            this.random = Objects.requireNonNull(source, "source");
            return this;
        }

        /** The policy in force: the custom one, else the scalar knobs' formula, else none (reservation left). */
        private IntToDoubleFunction resolveRetryDelay() {
            if (retryPolicy != null) {
                return retryPolicy;
            }
            if (retryBackoffSeconds == 0) {
                // No backoff configured (also skips pow(): 0 * Infinity would be NaN for a huge n).
                return null;
            }
            double base = retryBackoffSeconds;
            double multiplier = retryBackoffMultiplier;
            double max = retryBackoffMaxSeconds;
            boolean full = retryJitterFull;
            DoubleSupplier source = random;
            return n -> {
                double d = base * Math.pow(multiplier, Math.max(n, 1) - 1);
                if (Double.isNaN(d) || Double.isInfinite(d)) {
                    d = max > 0 ? max : Double.MAX_VALUE;
                }
                if (max > 0 && d > max) {
                    d = max;
                }
                return full ? d * source.getAsDouble() : d;
            };
        }

        public Builder onError(ErrorHandler handler) {
            this.onError = handler;
            return this;
        }

        public Builder onUnknownUrn(UnknownUrnHandler handler) {
            this.onUnknownUrn = handler;
            return this;
        }

        /** Test seam: replace the queue-state machine. */
        Builder ops(RedisQueueOps ops) {
            this.ops = ops;
            return this;
        }

        /** Test seam: the epoch-seconds clock. */
        Builder clock(DoubleSupplier clock) {
            this.clock = clock;
            return this;
        }

        /** Test seam: the monotonic clock and sleeper used by the poll wait. */
        Builder waitHooks(LongSupplier nanoClock, LongConsumer sleeper) {
            this.nanoClock = nanoClock;
            this.sleeper = sleeper;
            return this;
        }

        public RedisConsumer build() {
            return new RedisConsumer(this);
        }
    }
}
