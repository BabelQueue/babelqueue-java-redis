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

    private final RedisCommands<String, String> redis;
    private final String queue;
    private final String processing;
    private final Map<String, BabelHandler> handlers;
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
        String body = redis.blmove(queue, processing, LMoveArgs.Builder.leftRight(), blockTimeoutSeconds);
        if (body == null) {
            return 0; // nothing arrived within the block timeout
        }
        handle(body);
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

    private void handle(String body) {
        Envelope envelope = EnvelopeCodec.decode(body);

        if (!EnvelopeCodec.accepts(envelope)) {
            // Leave it on the processing list — a recovery sweep can triage/requeue it.
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Redis."),
                envelope, body);
            return;
        }

        String urn = EnvelopeCodec.urn(envelope);
        BabelHandler handler = handlers.get(urn);
        if (handler == null) {
            if (onUnknownUrn != null) {
                onUnknownUrn.onUnknownUrn(envelope, body);
                ack(body);
            } else {
                // Leave it on the processing list for a recovery sweep.
                report(new UnknownUrnException(urn), envelope, body);
            }
            return;
        }

        try {
            handler.handle(envelope, body);
            ack(body);
        } catch (Exception error) {
            // Leave the message on the processing list — at-least-once; a sweep requeues it.
            report(error, envelope, body);
        }
    }

    /** Remove one occurrence of the reserved body from the processing list (LREM). */
    private void ack(String body) {
        redis.lrem(processing, 1, body);
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
        private final Map<String, BabelHandler> handlers = new HashMap<>();
        private long blockTimeoutSeconds = 5;
        private ErrorHandler onError;
        private UnknownUrnHandler onUnknownUrn;

        private Builder(RedisCommands<String, String> redis, String queue) {
            this.redis = Objects.requireNonNull(redis, "redis");
            this.queue = Objects.requireNonNull(queue, "queue");
        }

        /** Register {@code handler} for {@code urn} (the last registration wins). */
        public Builder handler(String urn, BabelHandler handler) {
            this.handlers.put(urn, handler);
            return this;
        }

        public Builder handlers(Map<String, BabelHandler> handlers) {
            this.handlers.putAll(handlers);
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
