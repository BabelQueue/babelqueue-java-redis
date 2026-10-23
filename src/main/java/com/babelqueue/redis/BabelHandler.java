package com.babelqueue.redis;

import com.babelqueue.Envelope;

/** Processes one decoded, validated envelope and the raw JSON body it arrived on. */
@FunctionalInterface
public interface BabelHandler {

    /**
     * Handle a message. Returning normally acknowledges it (the consumer removes the
     * reservation); throwing leaves it reserved until its {@code retry_after} deadline passes, after which the
     * consumer-side sweep (legacy layout) or migrate (zset layout) returns it to the ready queue
     * (at-least-once).
     *
     * @param envelope the decoded, validated envelope
     * @param body     the raw envelope JSON the message arrived as (byte-identical to what was produced)
     */
    void handle(Envelope envelope, String body) throws Exception;
}
