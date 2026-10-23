/**
 * Redis transport for BabelQueue &mdash; a canonical-envelope {@link com.babelqueue.redis.RedisPublisher}
 * and a URN-routed {@link com.babelqueue.redis.RedisConsumer} over the Lettuce client,
 * on the framework-agnostic {@code babelqueue-core}.
 *
 * <p>It implements &sect;1 of the broker-bindings contract in two layouts
 * ({@link com.babelqueue.redis.RedisLayout}); the envelope is unchanged
 * ({@code schema_version} stays 1) in both.
 *
 * <ul>
 *   <li><strong>{@code LEGACY}</strong> (default) &mdash; the Java-owned reliable queue. The canonical envelope JSON
 *       is the list element, stored byte-for-byte. Reserving atomically moves the head of {@code <queue>} to
 *       {@code <queue>:processing} ({@code LMOVE}) and records a deadline in the sorted set
 *       {@code <queue>:processing:deadlines}; acking removes both. Before every reserve a sweep returns reservations
 *       whose deadline passed (a crashed worker's) to {@code <queue>}, body unchanged. The deadline is
 *       {@code retry_after} seconds (default 90; {@code null} = never expires).</li>
 *   <li><strong>{@code ZSET}</strong> &mdash; Laravel's Redis layout ({@code queues:<queue>}, {@code :delayed},
 *       {@code :reserved}, {@code :notify}), driven by Lua scripts through {@code EVALSHA}, so a Java worker and a
 *       Laravel worker can share one queue. Delayed publish and release use {@code :delayed}; an expired
 *       {@code :reserved} entry is recovered with {@code attempts + 1}. This layout carries no
 *       {@code __bq_frame} header frame, so out-of-band transport headers (W3C {@code traceparent}) are not carried
 *       (the envelope's {@code trace_id} still is).</li>
 * </ul>
 *
 * <p>Full spec: <a href="https://babelqueue.com">babelqueue.com</a>.
 */
package com.babelqueue.redis;
