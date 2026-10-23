package com.babelqueue.redis;

/** Internal helpers for Redis key layout. */
final class RedisQueues {

    /** Suffix of the per-queue reservation (in-flight) list (legacy layout). */
    static final String PROCESSING_SUFFIX = ":processing";

    /** Suffix of the deadline sorted set beside the processing list (legacy layout). */
    static final String DEADLINES_SUFFIX = ":processing:deadlines";

    /** Suffix of the dead-letter list (both layouts; never prefixed). */
    static final String DLQ_SUFFIX = ".dlq";

    /** Laravel's default key prefix ({@code queues:}) for the zset layout. */
    static final String DEFAULT_KEY_PREFIX = "queues:";

    private RedisQueues() {}

    /** The per-queue processing (in-flight) list key for {@code queue}. */
    static String processing(String queue) {
        return queue + PROCESSING_SUFFIX;
    }

    /** The deadline sorted set for {@code queue}'s processing list. */
    static String deadlines(String queue) {
        return queue + DEADLINES_SUFFIX;
    }

    /** The dead-letter list for {@code queue}. */
    static String dlq(String queue) {
        return queue + DLQ_SUFFIX;
    }

    /** The four key names of one queue in the zset (Laravel) layout, broker-bindings §1.1. */
    record LaravelKeys(String ready, String delayed, String reserved, String notifyKey) {}

    /** The zset-layout keys for {@code queue} under {@code prefix}. */
    static LaravelKeys laravelKeys(String prefix, String queue) {
        String ready = prefix + queue;
        return new LaravelKeys(ready, ready + ":delayed", ready + ":reserved", ready + ":notify");
    }
}
