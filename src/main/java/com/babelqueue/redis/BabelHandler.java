package com.babelqueue.redis;

import com.babelqueue.Envelope;

/** Processes one decoded, validated envelope and the raw JSON body it arrived on. */
@FunctionalInterface
public interface BabelHandler {

    /**
     * Handle a message. Returning normally acknowledges it (the consumer {@code LREM}s
     * it from the processing list); throwing leaves it on the processing list for a
     * recovery sweep to requeue (at-least-once).
     *
     * @param envelope the decoded, validated envelope
     * @param body     the raw envelope JSON the message arrived as (byte-identical to what was produced)
     */
    void handle(Envelope envelope, String body) throws Exception;
}
