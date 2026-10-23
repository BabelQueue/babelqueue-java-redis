package com.babelqueue.redis;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Map;
import java.util.Objects;
import java.util.function.DoubleSupplier;

/**
 * Sends canonical-envelope messages to one Redis queue (the §1 binding). Build one with
 * {@link #create} or {@link #builder}.
 *
 * <p>With {@link RedisLayout#ZSET} (Laravel's layout, opt-in) a publish is Laravel's push script
 * (list {@code RPUSH} plus a {@code :notify} token), and {@link #publishDelayed} adds to the {@code :delayed}
 * sorted set with score {@code floor(now) + ceil(delay)}. That layout has no header frame, so
 * {@link #publishWithHeaders} drops the headers (the envelope's {@code trace_id} still travels).
 *
 * <p>With the default {@link RedisLayout#LEGACY} layout the Redis list element <em>is</em> the canonical envelope JSON, stored byte-for-byte
 * with {@code RPUSH} — no wrapping, no added metadata (Redis lists have no native
 * attribute channel, so there is no property projection, §1.2). The body is byte-identical
 * to what every other broker carries for the same logical message (GR-5).
 *
 * <pre>{@code
 * RedisCommands<String, String> redis = RedisClient.create("redis://localhost:6379")
 *     .connect().sync();
 * RedisPublisher publisher = RedisPublisher.create(redis, "orders");
 * String id = publisher.publish("urn:babel:orders:created", Map.of("order_id", 1042));
 * }</pre>
 *
 * <p>To carry an out-of-band header (e.g. a W3C {@code traceparent}, ADR-0028) beside the
 * envelope — Redis having no native metadata channel — {@link #publishWithHeaders} wraps
 * the bare envelope in a transport-owned {@link RedisFrame} ({@code __bq_frame}) JSON
 * frame. A header-less publish still stores the bare envelope byte-for-byte, so the wire
 * envelope is never touched (GR-1) and pre-frame consumers keep working.
 */
public final class RedisPublisher {

    private final RedisCommands<String, String> redis;
    private final String queue;
    private final RedisQueueOps ops;

    private RedisPublisher(Builder builder) {
        this.redis = builder.redis;
        this.queue = builder.queue;
        this.ops = builder.ops != null
            ? builder.ops
            : new RedisQueueOps(builder.redis, builder.queue, builder.layout, builder.keyPrefix,
                RedisQueueOps.DEFAULT_RETRY_AFTER_SECONDS, -1, builder.clock);
    }

    /** A publisher for the {@code queue} ready list. */
    public static RedisPublisher create(RedisCommands<String, String> redis, String queue) {
        return builder(redis, queue).build();
    }

    public static Builder builder(RedisCommands<String, String> redis, String queue) {
        return new Builder(redis, queue);
    }

    /** Publish {@code (urn, data)} as a canonical envelope; returns the message id ({@code meta.id}). */
    public String publish(String urn, Map<String, Object> data) {
        return publish(urn, data, null);
    }

    /** Publish, continuing an existing {@code traceId} (or {@code null} to mint a fresh one). */
    public String publish(String urn, Map<String, Object> data, String traceId) {
        Envelope envelope = EnvelopeCodec.make(urn, data, queue, traceId);
        ops.push(EnvelopeCodec.encode(envelope));
        return envelope.meta().id();
    }

    /**
     * Publish so the message becomes available after {@code delaySeconds}: {@code ZADD <queue>:delayed} with
     * score {@code floor(now) + ceil(delaySeconds)} and no notify token (the next consumer migrate moves it).
     * Zset layout only; returns {@code meta.id}.
     *
     * @throws IllegalStateException    on the legacy layout, which has no delayed set
     * @throws IllegalArgumentException if {@code delaySeconds} is negative
     */
    public String publishDelayed(String urn, Map<String, Object> data, double delaySeconds) {
        return publishDelayed(urn, data, null, delaySeconds);
    }

    /** {@link #publishDelayed(String, Map, double)} continuing an existing {@code traceId}. */
    public String publishDelayed(String urn, Map<String, Object> data, String traceId, double delaySeconds) {
        Envelope envelope = EnvelopeCodec.make(urn, data, queue, traceId);
        ops.pushDelayed(EnvelopeCodec.encode(envelope), delaySeconds);
        return envelope.meta().id();
    }

    /**
     * Publish an already-built {@code envelope} together with out-of-band transport
     * {@code headers} (e.g. a W3C {@code traceparent}, ADR-0028). With usable headers it
     * {@code RPUSH}es a transport-owned {@link RedisFrame} JSON frame
     * ({@code {"__bq_frame":1,"headers":…,"body":<raw envelope>}}) wrapping the bare
     * envelope; with no/empty/blank headers it {@code RPUSH}es the bare envelope
     * byte-for-byte, byte-identical to {@link #publish}. The frozen envelope is never
     * touched (GR-1). On the zset layout the headers are dropped (no frame, A3). This is the produce-side seam the optional core
     * {@code com.babelqueue.otel.HeaderSender} wires to. Returns {@code meta.id}.
     */
    public String publishWithHeaders(Envelope envelope, Map<String, String> headers) {
        if (ops.layout() == RedisLayout.ZSET) {
            // Laravel's layout has no header frame (A3): headers are dropped, the envelope is pushed bare.
            ops.push(EnvelopeCodec.encode(envelope));
        } else {
            redis.rpush(queue, RedisFrame.frame(EnvelopeCodec.encode(envelope), headers));
        }
        return envelope.meta().id();
    }

    /** The ready-list key this publisher pushes to. */
    public String queue() {
        return queue;
    }

    /** Fluent builder for {@link RedisPublisher}. */
    public static final class Builder {
        private final RedisCommands<String, String> redis;
        private final String queue;
        private RedisLayout layout = RedisLayout.LEGACY;
        private String keyPrefix;
        private RedisQueueOps ops;
        private DoubleSupplier clock = () -> System.currentTimeMillis() / 1000.0;

        private Builder(RedisCommands<String, String> redis, String queue) {
            this.redis = Objects.requireNonNull(redis, "redis");
            this.queue = Objects.requireNonNull(queue, "queue");
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

        public RedisPublisher build() {
            return new RedisPublisher(this);
        }
    }
}
