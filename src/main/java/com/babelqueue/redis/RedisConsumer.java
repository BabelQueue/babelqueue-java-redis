package com.babelqueue.redis;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import io.lettuce.core.LMoveArgs;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Reserves messages from a Redis ready list, decodes and validates each, routes it to
 * the handler registered for its URN, and acks it on success. Reservation uses the §1
 * reliable-queue pattern: {@code BLMOVE <queue> <queue>:processing LEFT RIGHT} atomically
 * moves the head of the ready list to the tail of a per-queue processing list, so an
 * in-flight message survives a worker crash. A successful handler {@code LREM}s the
 * message from the processing list; a throwing handler leaves it there for a recovery
 * sweep to requeue (at-least-once). The poll loop never stops on a bad message — observe
 * via {@code onError}/{@code onUnknownUrn}.
 *
 * <p>Redis lists carry no native delivery counter (unlike SQS's {@code ApproximateReceiveCount}
 * or AMQP's {@code DeliveryCount}), so {@code attempts} is taken from the envelope body
 * unchanged — there is no broker-side reconciliation to apply (the §1 manifest block locks
 * only payload identity; attempts handling is runtime-specific).
 *
 * <p>A reserved value carrying out-of-band headers (e.g. a W3C {@code traceparent},
 * ADR-0028) is wrapped in a transport-owned {@link RedisFrame} ({@code __bq_frame}) JSON
 * frame; the consumer unframes it transparently, decodes the bare envelope and surfaces the
 * headers to a {@link HeaderHandler}. A bare (un-framed) value consumes exactly as before
 * with empty headers, so cross-version queues interoperate. The {@code LREM} ack handle is
 * always the stored value (frame or bare), so reservation accounting is unaffected.
 *
 * <p>This is a Java-owned reliable queue; full parity with Laravel's reserved-sorted-set
 * reservation on a <em>shared</em> Redis queue is a separate task (broker-bindings §1.4).
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

    private final RedisCommands<String, String> redis;
    private final String queue;
    private final String processing;
    private final Map<String, Registered> handlers;
    private final long blockTimeoutSeconds;
    private final ErrorHandler onError;
    private final UnknownUrnHandler onUnknownUrn;

    private RedisConsumer(Builder builder) {
        this.redis = builder.redis;
        this.queue = builder.queue;
        this.processing = RedisQueues.processing(builder.queue);
        this.handlers = Map.copyOf(builder.handlers);
        this.blockTimeoutSeconds = builder.blockTimeoutSeconds;
        this.onError = builder.onError;
        this.onUnknownUrn = builder.onUnknownUrn;
    }

    public static Builder builder(RedisCommands<String, String> redis, String queue) {
        return new Builder(redis, queue);
    }

    /**
     * Reserve at most one message (blocking up to the configured timeout), route it, and
     * ack it when handled. Returns the number of messages reserved this call (0 or 1).
     */
    public int poll() {
        String value = redis.blmove(queue, processing, LMoveArgs.Builder.leftRight(), blockTimeoutSeconds);
        if (value == null) {
            return 0; // nothing arrived within the block timeout
        }
        handle(value);
        return 1;
    }

    /** Poll until the current thread is interrupted (each poll blocks, so this does not busy-loop). */
    public void run() {
        run(() -> !Thread.currentThread().isInterrupted());
    }

    /** Poll while {@code shouldContinue} returns true. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            poll();
        }
    }

    private void handle(String value) {
        // The reserved value may be a transport-owned header frame (ADR-0028) or a bare
        // envelope. Unframe to recover the bare envelope body + out-of-band headers, but
        // ack against the original reserved value — the LREM handle is the stored string.
        RedisFrame.Unframed unframed = RedisFrame.unframe(value);
        String body = unframed.body();
        Map<String, String> headers = unframed.headers();
        Envelope envelope = EnvelopeCodec.decode(body);

        if (!EnvelopeCodec.accepts(envelope)) {
            // Leave it on the processing list — a recovery sweep can triage/requeue it.
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Redis."),
                envelope, body);
            return;
        }

        String urn = EnvelopeCodec.urn(envelope);
        Registered handler = handlers.get(urn);
        if (handler == null) {
            if (onUnknownUrn != null) {
                onUnknownUrn.onUnknownUrn(envelope, body);
                ack(value);
            } else {
                // Leave it on the processing list for a recovery sweep.
                report(new UnknownUrnException(urn), envelope, body);
            }
            return;
        }

        try {
            handler.handle(envelope, body, headers);
            ack(value);
        } catch (Exception error) {
            // Leave the message on the processing list — at-least-once; a sweep requeues it.
            report(error, envelope, body);
        }
    }

    /** Remove one occurrence of the reserved value from the processing list (LREM). */
    private void ack(String value) {
        redis.lrem(processing, 1, value);
    }

    private void report(Throwable error, Envelope envelope, String body) {
        if (onError != null) {
            onError.onError(error, envelope, body);
        }
    }

    /** The ready-list key this consumer reserves from. */
    public String queue() {
        return queue;
    }

    /** The per-queue processing (in-flight) list key. */
    public String processingQueue() {
        return processing;
    }

    /** Fluent builder for {@link RedisConsumer}. */
    public static final class Builder {
        private final RedisCommands<String, String> redis;
        private final String queue;
        private final Map<String, Registered> handlers = new HashMap<>();
        private long blockTimeoutSeconds = 5;
        private ErrorHandler onError;
        private UnknownUrnHandler onUnknownUrn;

        private Builder(RedisCommands<String, String> redis, String queue) {
            this.redis = Objects.requireNonNull(redis, "redis");
            this.queue = Objects.requireNonNull(queue, "queue");
        }

        /** Register {@code handler} for {@code urn} (the last registration wins). */
        public Builder handler(String urn, BabelHandler handler) {
            this.handlers.put(urn, (env, body, headers) -> handler.handle(env, body));
            return this;
        }

        /**
         * Register a header-aware {@code handler} for {@code urn} (the last registration
         * wins) — it additionally receives the message's out-of-band transport headers,
         * the seam for OpenTelemetry {@code traceparent} propagation (ADR-0028).
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
         * Seconds each {@code BLMOVE} blocks waiting for a message (default 5; {@code 0}
         * blocks indefinitely).
         */
        public Builder blockTimeoutSeconds(long seconds) {
            this.blockTimeoutSeconds = seconds;
            return this;
        }

        public Builder onError(ErrorHandler handler) {
            this.onError = handler;
            return this;
        }

        public Builder onUnknownUrn(UnknownUrnHandler handler) {
            this.onUnknownUrn = handler;
            return this;
        }

        public RedisConsumer build() {
            return new RedisConsumer(this);
        }
    }
}
