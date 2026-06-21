package com.babelqueue.redis;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Map;
import java.util.Objects;

/**
 * Sends canonical-envelope messages to one Redis list (the §1 reliable-queue pattern).
 * Build one with {@link #create} or {@link #builder}.
 *
 * <p>The Redis list element <em>is</em> the canonical envelope JSON, stored byte-for-byte
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

    private RedisPublisher(Builder builder) {
        this.redis = builder.redis;
        this.queue = builder.queue;
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
        redis.rpush(queue, EnvelopeCodec.encode(envelope));
        return envelope.meta().id();
    }

    /**
     * Publish an already-built {@code envelope} together with out-of-band transport
     * {@code headers} (e.g. a W3C {@code traceparent}, ADR-0028). With usable headers it
     * {@code RPUSH}es a transport-owned {@link RedisFrame} JSON frame
     * ({@code {"__bq_frame":1,"headers":…,"body":<raw envelope>}}) wrapping the bare
     * envelope; with no/empty/blank headers it {@code RPUSH}es the bare envelope
     * byte-for-byte, byte-identical to {@link #publish}. The frozen envelope is never
     * touched (GR-1). This is the produce-side seam the optional core
     * {@code com.babelqueue.otel.HeaderSender} wires to. Returns {@code meta.id}.
     */
    public String publishWithHeaders(Envelope envelope, Map<String, String> headers) {
        redis.rpush(queue, RedisFrame.frame(EnvelopeCodec.encode(envelope), headers));
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

        private Builder(RedisCommands<String, String> redis, String queue) {
            this.redis = Objects.requireNonNull(redis, "redis");
            this.queue = Objects.requireNonNull(queue, "queue");
        }

        public RedisPublisher build() {
            return new RedisPublisher(this);
        }
    }
}
