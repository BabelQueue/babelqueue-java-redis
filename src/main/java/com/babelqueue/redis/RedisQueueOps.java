package com.babelqueue.redis;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.CommandType;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.DoubleSupplier;

/**
 * The queue-state machine behind {@link RedisConsumer} and {@link RedisPublisher}: reserve, ack,
 * release, dead-letter, migrate and sweep for both layouts (broker-bindings §1.1, §1.7).
 *
 * <p>Every method that depends on the clock has an explicit-{@code now} variant (epoch seconds, may be
 * fractional) so the cross-SDK behavioural suite can pin time; the plain variants read the injected clock.
 * Multi-key transitions run as single Lua scripts ({@link RedisLua}).
 */
class RedisQueueOps {

    private static final System.Logger LOG = System.getLogger(RedisQueueOps.class.getName());

    /** Default seconds before an unacknowledged reservation is recovered. */
    static final long DEFAULT_RETRY_AFTER_SECONDS = 90;

    private final RedisCommands<String, String> redis;
    private final LuaExecutor lua;
    private final String queue;
    private final RedisLayout layout;
    private final RedisQueues.LaravelKeys keys;
    private final Long retryAfter;
    private final long migrationBatchSize;
    private final DoubleSupplier clock;
    private final SecureRandom random = new SecureRandom();

    /**
     * @param keyPrefix          zset-layout key prefix ({@code null} = {@code queues:}); ignored for legacy
     * @param retryAfterSeconds  seconds before a reservation is recovered; {@code null} disables recovery
     *                           (the deadline score is {@code +inf}); negative is rejected
     * @param migrationBatchSize max items moved per migrate / sweep call; {@code -1} = unbounded
     * @param clock              epoch seconds
     */
    RedisQueueOps(
        RedisCommands<String, String> redis,
        String queue,
        RedisLayout layout,
        String keyPrefix,
        Long retryAfterSeconds,
        long migrationBatchSize,
        DoubleSupplier clock) {
        this.redis = Objects.requireNonNull(redis, "redis");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.layout = Objects.requireNonNull(layout, "layout");
        if (retryAfterSeconds != null && retryAfterSeconds < 0) {
            throw new IllegalArgumentException("retryAfter must not be negative, got " + retryAfterSeconds);
        }
        if (migrationBatchSize == 0 || migrationBatchSize < -1) {
            throw new IllegalArgumentException("migrationBatchSize must be -1 (unbounded) or positive, got "
                + migrationBatchSize);
        }
        this.retryAfter = retryAfterSeconds;
        this.migrationBatchSize = migrationBatchSize;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.keys = RedisQueues.laravelKeys(keyPrefix == null ? RedisQueues.DEFAULT_KEY_PREFIX : keyPrefix, queue);
        this.lua = new LuaExecutor(redis);
    }

    RedisLayout layout() {
        return layout;
    }

    String queue() {
        return queue;
    }

    /** The ready-list key: {@code <prefix><queue>} (zset) or the raw queue name (legacy). */
    String readyKey() {
        return layout == RedisLayout.ZSET ? keys.ready() : queue;
    }

    /** The in-flight key: {@code :reserved} sorted set (zset) or {@code :processing} list (legacy). */
    String inFlightKey() {
        return layout == RedisLayout.ZSET ? keys.reserved() : RedisQueues.processing(queue);
    }

    String delayedKey() {
        return keys.delayed();
    }

    String notifyKey() {
        return keys.notifyKey();
    }

    String deadlinesKey() {
        return RedisQueues.deadlines(queue);
    }

    String dlqKey() {
        return RedisQueues.dlq(queue);
    }

    double now() {
        return clock.getAsDouble();
    }

    // ----------------------------------------------------------------------------------------- scores

    /** {@code floor(now) + ceil(delay)} — the {@code :delayed} score (broker-bindings §1.6). */
    static long delayScore(double now, double delay) {
        return (long) Math.floor(now) + (long) Math.ceil(delay);
    }

    /** The reservation deadline score argument: {@code floor(now) + retry_after}, or {@code +inf} when disabled. */
    String deadlineArg(double now) {
        return retryAfter == null ? "+inf" : Long.toString((long) Math.floor(now) + retryAfter);
    }

    // ---------------------------------------------------------------------------------------- reserve

    /** Reserve the head of the queue (migrating / sweeping first), or {@code null} when empty. */
    Reservation reserve() {
        return reserveAt(now());
    }

    Reservation reserveAt(double now) {
        return layout == RedisLayout.ZSET ? reserveZset(now) : reserveLegacy(now);
    }

    private Reservation reserveZset(double now) {
        migrateAt(now);
        // Read as raw bytes: a stored value that is not valid UTF-8 must reach the poison DLQ record intact
        // (ADR-0035 section 1), which a text-decoded reply would already have replaced with U+FFFD.
        List<Object> reply = lua.runBytes(
            RedisLua.ZSET_RESERVE,
            ScriptOutputType.MULTI,
            new String[] {keys.ready(), keys.reserved(), keys.notifyKey()},
            deadlineArg(now));
        if (reply == null || reply.size() < 2 || reply.get(0) == null || reply.get(1) == null) {
            return null;
        }
        byte[] job = toBytes(reply.get(0));
        byte[] reserved = toBytes(reply.get(1));
        String jobText = Reservation.exactText(job);
        String reservedText = Reservation.exactText(reserved);
        return new Reservation(
            jobText != null ? jobText : new String(job, StandardCharsets.UTF_8),
            reservedText != null ? reservedText : new String(reserved, StandardCharsets.UTF_8),
            Arrays.equals(job, reserved),
            jobText != null ? null : job,
            reservedText != null ? null : reserved);
    }

    private static byte[] toBytes(Object reply) {
        return reply instanceof byte[] bytes ? bytes : reply.toString().getBytes(StandardCharsets.UTF_8);
    }

    private Reservation reserveLegacy(double now) {
        sweepAt(now);
        String token = newToken();
        byte[] raw = lua.runBytes(
            RedisLua.LEGACY_RESERVE,
            ScriptOutputType.VALUE,
            new String[] {queue, RedisQueues.processing(queue), RedisQueues.deadlines(queue)},
            deadlineArg(now),
            token);
        if (raw == null) {
            return null;
        }
        String text = Reservation.exactText(raw);
        return text != null
            ? new Reservation(text, token, false)
            : new Reservation(new String(raw, StandardCharsets.UTF_8), token, false, raw, null);
    }

    /** A fresh 128-bit reservation token: 32 lowercase hex digits from {@link SecureRandom}. */
    String newToken() {
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        StringBuilder hex = new StringBuilder(32);
        for (byte b : bytes) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }

    /**
     * The delivery count the handler is shown: zset = stored {@code attempts + 1} (what Laravel's reserve
     * wrote), {@code 1} for a poison pop; legacy = the stored {@code attempts} unchanged.
     */
    int deliveryCount(Reservation reservation) {
        Envelope envelope = EnvelopeCodec.decode(reservation.body(), w -> {});
        int stored = Math.max(envelope.attempts(), 0);
        if (layout == RedisLayout.LEGACY) {
            return stored;
        }
        return reservation.poison() ? 1 : stored + 1;
    }

    // --------------------------------------------------------------------------- migrate / sweep

    /** Move due {@code :delayed} (and expired {@code :reserved}) members to the ready list; zset only. */
    long migrateAt(double now) {
        if (layout != RedisLayout.ZSET) {
            return 0;
        }
        long moved = migrate(keys.delayed(), now);
        if (retryAfter != null) {
            moved += migrate(keys.reserved(), now);
        }
        return moved;
    }

    private long migrate(String source, double now) {
        Long moved = lua.run(
            RedisLua.ZSET_MIGRATE,
            ScriptOutputType.INTEGER,
            new String[] {source, keys.ready(), keys.notifyKey()},
            Long.toString((long) Math.floor(now)),
            Long.toString(migrationBatchSize));
        return moved == null ? 0 : moved;
    }

    /** Return expired legacy reservations to the ready list; legacy only. Runs even when recovery is off. */
    long sweepAt(double now) {
        if (layout != RedisLayout.LEGACY) {
            return 0;
        }
        Long moved = lua.run(
            RedisLua.LEGACY_SWEEP,
            ScriptOutputType.INTEGER,
            new String[] {queue, RedisQueues.processing(queue), RedisQueues.deadlines(queue)},
            Long.toString((long) Math.floor(now)),
            Long.toString(migrationBatchSize));
        return moved == null ? 0 : moved;
    }

    // -------------------------------------------------------------------- ack / release / dead-letter

    /** Acknowledge: remove the reservation. Legacy returns {@code false} (and warns) when it already expired. */
    boolean ack(Reservation reservation) {
        if (layout == RedisLayout.ZSET) {
            Long removed = reservation.handleBytes() != null
                ? lua.runBytesCommand(CommandType.ZREM, keys.reserved(), reservation.handleBytes())
                : redis.zrem(keys.reserved(), reservation.handle());
            return removed != null && removed > 0;
        }
        Long done = reservation.hasInvalidUtf8()
            ? lua.runBytes(
                RedisLua.LEGACY_ACK,
                ScriptOutputType.INTEGER,
                new String[] {RedisQueues.processing(queue), RedisQueues.deadlines(queue)},
                reservation.exactLegacyMember(),
                reservation.exactBody())
            : lua.run(
                RedisLua.LEGACY_ACK,
                ScriptOutputType.INTEGER,
                new String[] {RedisQueues.processing(queue), RedisQueues.deadlines(queue)},
                reservation.legacyMember(),
                reservation.body());
        return gate(done, "ack");
    }

    /**
     * Schedule the reserved message for redelivery after {@code delay} seconds with {@code attempts + 1}
     * (zset, A2). Legacy supports only {@code delay <= 0} (straight back to the ready list).
     */
    void release(Reservation reservation, double delay) {
        releaseAt(reservation, delay, now());
    }

    void releaseAt(Reservation reservation, double delay, double now) {
        if (delay < 0) {
            throw new IllegalArgumentException("delay must not be negative, got " + delay);
        }
        if (layout == RedisLayout.ZSET) {
            if (reservation.poison() || reservation.hasInvalidUtf8() || reservation.handleBytes() != null) {
                throw new IllegalStateException("a poison reservation cannot be released; dead-letter it");
            }
            lua.run(
                RedisLua.ZSET_RELEASE,
                ScriptOutputType.INTEGER,
                new String[] {keys.reserved(), keys.delayed()},
                reservation.handle(),
                Long.toString(delayScore(now, delay)),
                releaseBody(reservation.body()));
            return;
        }
        if (delay > 0) {
            throw new UnsupportedOperationException(
                "a delayed release needs the zset layout; the legacy layout only releases immediately");
        }
        if (reservation.hasInvalidUtf8()) {
            // Not valid UTF-8, so never an envelope: re-queue the exact stored bytes, nothing to re-encode.
            Long moved = lua.runBytes(
                RedisLua.LEGACY_MOVE,
                ScriptOutputType.INTEGER,
                new String[] {RedisQueues.processing(queue), RedisQueues.deadlines(queue), queue},
                reservation.exactLegacyMember(),
                reservation.exactBody(),
                reservation.exactBody());
            gate(moved, "release");
            return;
        }
        RedisFrame.Unframed unframed = RedisFrame.unframe(reservation.body());
        String body = RedisFrame.frame(releaseBody(unframed.body()), unframed.headers());
        Long done = lua.run(
            RedisLua.LEGACY_MOVE,
            ScriptOutputType.INTEGER,
            new String[] {RedisQueues.processing(queue), RedisQueues.deadlines(queue), queue},
            reservation.legacyMember(),
            reservation.body(),
            body);
        gate(done, "release");
    }

    /** Move the reserved message to {@code <queue>.dlq} (raw body, byte-for-byte) and remove the reservation. */
    boolean deadLetter(Reservation reservation) {
        if (reservation.hasInvalidUtf8()) {
            return deadLetterBytes(reservation, reservation.bodyBytes());
        }
        return deadLetter(reservation, reservation.body());
    }

    /**
     * Move the reserved message to {@code <queue>.dlq} writing {@code dlqBody} (e.g. an envelope annotated with
     * a {@code dead_letter} block) and remove the reservation, atomically and gated like an ack: when the
     * reservation is already gone nothing is written. The reservation itself is always identified by its
     * stored {@link Reservation#body()}.
     */
    boolean deadLetter(Reservation reservation, String dlqBody) {
        if (reservation.hasInvalidUtf8() || reservation.handleBytes() != null) {
            // The reservation is identified by bytes that are not valid UTF-8: send them exactly as stored.
            return deadLetterBytes(reservation, dlqBody.getBytes(StandardCharsets.UTF_8));
        }
        if (layout == RedisLayout.ZSET) {
            Long done = lua.run(
                RedisLua.ZSET_DEAD_LETTER,
                ScriptOutputType.INTEGER,
                new String[] {keys.reserved(), RedisQueues.dlq(queue)},
                reservation.handle(),
                dlqBody);
            return done != null && done == 1;
        }
        Long done = lua.run(
            RedisLua.LEGACY_MOVE,
            ScriptOutputType.INTEGER,
            new String[] {RedisQueues.processing(queue), RedisQueues.deadlines(queue), RedisQueues.dlq(queue)},
            reservation.legacyMember(),
            reservation.body(),
            dlqBody);
        return gate(done, "dead-letter");
    }

    private boolean deadLetterBytes(Reservation reservation, byte[] dlqBody) {
        if (layout == RedisLayout.ZSET) {
            Long done = lua.runBytes(
                RedisLua.ZSET_DEAD_LETTER,
                ScriptOutputType.INTEGER,
                new String[] {keys.reserved(), RedisQueues.dlq(queue)},
                reservation.exactHandle(),
                dlqBody);
            return done != null && done == 1;
        }
        Long done = lua.runBytes(
            RedisLua.LEGACY_MOVE,
            ScriptOutputType.INTEGER,
            new String[] {RedisQueues.processing(queue), RedisQueues.deadlines(queue), RedisQueues.dlq(queue)},
            reservation.exactLegacyMember(),
            reservation.exactBody(),
            dlqBody);
        return gate(done, "dead-letter");
    }

    private boolean gate(Long done, String op) {
        if (done != null && done == 1) {
            return true;
        }
        LOG.log(System.Logger.Level.WARNING,
            "reservation expired: {0} on queue {1} touched nothing (the reservation was swept or already settled)",
            op, queue);
        return false;
    }

    /**
     * The body re-queued on release: the SDK codec's encoding of the message with {@code attempts + 1}.
     * Unknown keys, their values and types (incl. {@code {}} vs {@code []}) are preserved (ADR-0031); a body
     * that is not a conformant envelope is returned unchanged.
     */
    static String releaseBody(String raw) {
        Envelope envelope = EnvelopeCodec.decode(raw, w -> {});
        if (!EnvelopeCodec.accepts(envelope)) {
            return raw;
        }
        return EnvelopeCodec.encode(envelope.withAttempts(Math.max(envelope.attempts(), 0) + 1));
    }

    // ------------------------------------------------------------------------------------ publishing

    /** Publish {@code body} to the ready list (zset: also a {@code :notify} token, atomically). */
    void push(String body) {
        if (layout == RedisLayout.ZSET) {
            lua.run(RedisLua.ZSET_PUSH, ScriptOutputType.INTEGER, new String[] {keys.ready(), keys.notifyKey()}, body);
        } else {
            redis.rpush(queue, body);
        }
    }

    /** Schedule {@code body} on {@code :delayed} for {@code floor(now) + ceil(delay)}; zset only, no notify token. */
    void pushDelayed(String body, double delay) {
        pushDelayedAt(body, delay, now());
    }

    void pushDelayedAt(String body, double delay, double now) {
        if (layout != RedisLayout.ZSET) {
            throw new IllegalStateException("delayed publish needs the zset layout");
        }
        if (delay < 0) {
            throw new IllegalArgumentException("delay must not be negative, got " + delay);
        }
        redis.zadd(keys.delayed(), (double) delayScore(now, delay), body);
    }

    // ----------------------------------------------------------------------------------------- wait

    /**
     * Zset only: block on the {@code :notify} list for up to {@code seconds} (&le; 1 in the consumer, so a
     * due delayed / expired reserved item is noticed within a second). The token only wakes the consumer;
     * the reserve script pops one if present.
     */
    void awaitNotify(double seconds) {
        redis.blpop(Math.max(seconds, 0.01), keys.notifyKey());
    }
}
