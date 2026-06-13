/**
 * Redis transport for BabelQueue — a canonical-envelope {@link com.babelqueue.redis.RedisPublisher}
 * and a URN-routed {@link com.babelqueue.redis.RedisConsumer} over the Lettuce client,
 * on the framework-agnostic {@code babelqueue-core}.
 *
 * <p>It implements §1 of the broker-bindings contract using the reliable-queue list
 * pattern: the canonical envelope JSON <em>is</em> the Redis list element, stored
 * byte-for-byte with no wrapping and no added metadata (Redis lists carry no native
 * attribute channel, so there is no property projection). Producing is {@code RPUSH};
 * reserving moves the head of the ready list to a per-queue {@code <queue>:processing}
 * list with {@code BLMOVE} (so an in-flight message survives a worker crash); acking
 * is {@code LREM} of that processing list. The envelope is unchanged
 * ({@code schema_version} stays 1); Redis is purely additive.
 *
 * <p>This is a <strong>Java-owned reliable queue</strong>: it mirrors the Go runtime's
 * reliable-queue mechanism. Full parity with Laravel's reserved-sorted-set reservation
 * on a <em>shared</em> PHP+Java Redis queue is a separate task — a consumer reading a
 * queue produced by the Laravel driver must replicate Laravel's reserve/ack semantics
 * (see broker-bindings §1.4). Pointed at a queue this SDK owns end-to-end, it is a
 * complete, crash-safe transport.
 *
 * <p>Full spec: <a href="https://babelqueue.com">babelqueue.com</a>.
 */
package com.babelqueue.redis;
