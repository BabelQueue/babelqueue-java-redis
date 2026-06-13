package com.babelqueue.redis;

/** Internal helpers for Redis key layout. */
final class RedisQueues {

    /** Suffix of the per-queue reservation (in-flight) list. */
    static final String PROCESSING_SUFFIX = ":processing";

    private RedisQueues() {}

    /** The per-queue processing (in-flight) list key for {@code queue}. */
    static String processing(String queue) {
        return queue + PROCESSING_SUFFIX;
    }
}
