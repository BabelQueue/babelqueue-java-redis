package com.babelqueue.redis;

/**
 * Which Redis key layout a publisher or consumer uses (broker-bindings §1).
 *
 * <ul>
 *   <li>{@link #LEGACY} (the default) is the Java-owned reliable queue: a ready list
 *       {@code <queue>}, an in-flight list {@code <queue>:processing} and a deadline sorted set
 *       {@code <queue>:processing:deadlines} that lets a killed worker's reservations be swept
 *       back to the ready list.</li>
 *   <li>{@link #ZSET} is Laravel's layout on a shared queue: {@code queues:<queue>},
 *       {@code :delayed}, {@code :reserved} and {@code :notify}. It is opt-in.</li>
 * </ul>
 */
public enum RedisLayout {
    /** The Java-owned {@code :processing} list with a deadline-zset sweep (default). */
    LEGACY,
    /** Laravel's ready list + {@code :delayed}/{@code :reserved} sorted sets + {@code :notify} list. */
    ZSET
}
