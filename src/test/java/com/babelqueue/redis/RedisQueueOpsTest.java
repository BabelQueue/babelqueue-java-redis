package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import io.lettuce.core.ScriptOutputType;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** The queue-state machine against a recording mock: keys, arguments, scores and reply handling. */
class RedisQueueOpsTest {

    private static final Pattern TOKEN = Pattern.compile("[0-9a-f]{32}");
    private static final String BODY = EnvelopeCodec.encode(
        EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 1042), "orders", "trace-1"));

    private static RedisQueueOps ops(RecordingRedis rec, RedisLayout layout, Long retryAfter, long batch,
        double now) {
        return new RedisQueueOps(rec.redis, "orders", layout, null, retryAfter, batch, () -> now);
    }

    private static RedisQueueOps zset(RecordingRedis rec) {
        return ops(rec, RedisLayout.ZSET, 90L, -1, 1000.4);
    }

    private static RedisQueueOps legacy(RecordingRedis rec) {
        return ops(rec, RedisLayout.LEGACY, 90L, -1, 1000.4);
    }

    // ------------------------------------------------------------------------------- construction

    @Test
    void constructorValidatesItsArguments() {
        RecordingRedis rec = new RecordingRedis();
        assertThrows(NullPointerException.class,
            () -> new RedisQueueOps(null, "q", RedisLayout.ZSET, null, 90L, -1, () -> 0));
        assertThrows(NullPointerException.class,
            () -> new RedisQueueOps(rec.redis, null, RedisLayout.ZSET, null, 90L, -1, () -> 0));
        assertThrows(NullPointerException.class,
            () -> new RedisQueueOps(rec.redis, "q", null, null, 90L, -1, () -> 0));
        assertThrows(NullPointerException.class,
            () -> new RedisQueueOps(rec.redis, "q", RedisLayout.ZSET, null, 90L, -1, null));
        assertThrows(IllegalArgumentException.class, () -> ops(rec, RedisLayout.ZSET, -1L, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> ops(rec, RedisLayout.ZSET, 90L, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> ops(rec, RedisLayout.ZSET, 90L, -2, 0));
        ops(rec, RedisLayout.ZSET, null, 100, 0); // null retry_after is the "disabled" value, not an error
        ops(rec, RedisLayout.ZSET, 0L, 1, 0);
    }

    @Test
    void keyNamesFollowTheLayoutAndPrefix() {
        RecordingRedis rec = new RecordingRedis();
        RedisQueueOps z = zset(rec);
        assertEquals(RedisLayout.ZSET, z.layout());
        assertEquals("orders", z.queue());
        assertEquals("queues:orders", z.readyKey());
        assertEquals("queues:orders:reserved", z.inFlightKey());
        assertEquals("queues:orders:delayed", z.delayedKey());
        assertEquals("queues:orders:notify", z.notifyKey());
        assertEquals("orders:processing:deadlines", z.deadlinesKey());
        assertEquals("orders.dlq", z.dlqKey());
        assertEquals(1000.4, z.now());

        RedisQueueOps l = legacy(rec);
        assertEquals(RedisLayout.LEGACY, l.layout());
        assertEquals("orders", l.readyKey());
        assertEquals("orders:processing", l.inFlightKey());

        RedisQueueOps custom = new RedisQueueOps(rec.redis, "orders", RedisLayout.ZSET, "app:", 90L, -1, () -> 0);
        assertEquals("app:orders", custom.readyKey());
        assertEquals("app:orders:reserved", custom.inFlightKey());
    }

    // ------------------------------------------------------------------------------------- scores

    @Test
    void delayScoreIsFloorNowPlusCeilDelay() {
        assertEquals(1002, RedisQueueOps.delayScore(1000.7, 1.5));
        assertEquals(1000, RedisQueueOps.delayScore(1000.0, 0));
        assertEquals(1001, RedisQueueOps.delayScore(1000.0, 0.2));
        assertEquals(1010, RedisQueueOps.delayScore(1000.99, 10));
    }

    @Test
    void deadlineArgIsFloorNowPlusRetryAfterOrPlusInf() {
        RecordingRedis rec = new RecordingRedis();
        assertEquals("1090", zset(rec).deadlineArg(1000.4));
        assertEquals("+inf", ops(rec, RedisLayout.LEGACY, null, -1, 0).deadlineArg(1000.4));
        assertEquals("1000", ops(rec, RedisLayout.ZSET, 0L, -1, 0).deadlineArg(1000.9));
    }

    @Test
    void tokensAre32LowercaseHexAndDistinct() {
        RedisQueueOps ops = legacy(new RecordingRedis());
        String a = ops.newToken();
        String b = ops.newToken();
        assertTrue(TOKEN.matcher(a).matches(), a);
        assertTrue(TOKEN.matcher(b).matches(), b);
        assertNotEquals(a, b);
    }

    // ------------------------------------------------------------------------------- zset reserve

    /**
     * lua-cjson decodes numbers it cannot encode ({@code 1e400}, {@code NaN}) and Redis Lua has no rollback, so
     * the reserve script must guard the re-encode with {@code pcall} - never a bare {@code cjson.encode}
     * after the {@code LPOP} (broker-bindings 1.1: the script MUST NOT fail after the pop).
     */
    @Test
    void zsetReserveScriptGuardsTheReEncodeWithPcall() {
        String script = RedisLua.ZSET_RESERVE;
        assertTrue(script.contains("pcall(cjson.encode, payload)"), "the re-encode is not pcall-guarded");
        assertTrue(script.contains("pcall(cjson.decode, job)"), "the decode is not pcall-guarded");
        assertFalse(Pattern.compile("(?<!pcall\\()cjson\\.encode\\(").matcher(script).find(),
            "an unprotected cjson.encode(...) call remains in the reserve script");
        int lpop = script.indexOf("redis.call('lpop', KEYS[1])");
        int encode = script.indexOf("pcall(cjson.encode");
        int zadd = script.indexOf("redis.call('zadd', KEYS[2], ARGV[1], reserved)");
        int poisonZadd = script.indexOf("redis.call('zadd', KEYS[2], ARGV[1], job)");
        assertTrue(lpop >= 0 && lpop < encode && encode < zadd && zadd < poisonZadd,
            "unexpected script shape: the encode failure must fall through to the raw poison write");
    }

    @Test
    void zsetReserveMigratesDelayedThenReservedThenPops() {
        RecordingRedis rec = new RecordingRedis()
            .script(RedisLua.ZSET_MIGRATE, 0L)
            .script(RedisLua.ZSET_RESERVE, List.of(BODY, "reserved-copy"));

        Reservation r = zset(rec).reserve();

        assertEquals(new Reservation(BODY, "reserved-copy", false), r);
        List<RecordingRedis.Call> migrate = rec.scriptCalls(RedisLua.ZSET_MIGRATE);
        assertEquals(2, migrate.size());
        assertArrayEquals(new String[] {"queues:orders:delayed", "queues:orders", "queues:orders:notify"},
            migrate.get(0).keys());
        assertArrayEquals(new String[] {"queues:orders:reserved", "queues:orders", "queues:orders:notify"},
            migrate.get(1).keys());
        assertEquals(List.of("1000", "-1"), migrate.get(0).values());
        assertEquals(ScriptOutputType.INTEGER, RecordingRedis.outputType(migrate.get(0)));

        RecordingRedis.Call pop = rec.scriptCalls(RedisLua.ZSET_RESERVE).get(0);
        assertArrayEquals(new String[] {"queues:orders", "queues:orders:reserved", "queues:orders:notify"},
            pop.keys());
        assertEquals(List.of("1090"), pop.values());
        assertEquals(ScriptOutputType.MULTI, RecordingRedis.outputType(pop));
    }

    @Test
    void zsetReserveFlagsAPoisonPop() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.ZSET_RESERVE, List.of("not-json", "not-json"));
        Reservation r = zset(rec).reserve();
        assertTrue(r.poison());
        assertEquals("not-json", r.body());
    }

    @Test
    void zsetReserveOnAnEmptyQueueReturnsNull() {
        assertNull(zset(new RecordingRedis()).reserve());
        assertNull(zset(new RecordingRedis().script(RedisLua.ZSET_RESERVE, List.of("only-one"))).reserve());
        assertNull(zset(new RecordingRedis().script(RedisLua.ZSET_RESERVE, java.util.Arrays.asList("a", null)))
            .reserve());
    }

    @Test
    void zsetReserveSkipsTheReservedMigrationWhenRecoveryIsDisabled() {
        RecordingRedis rec = new RecordingRedis();
        ops(rec, RedisLayout.ZSET, null, 50, 1000.4).reserve();
        List<RecordingRedis.Call> migrate = rec.scriptCalls(RedisLua.ZSET_MIGRATE);
        assertEquals(1, migrate.size());
        assertEquals("queues:orders:delayed", migrate.get(0).keys()[0]);
        assertEquals(List.of("1000", "50"), migrate.get(0).values());
        assertEquals(List.of("+inf"), rec.scriptCalls(RedisLua.ZSET_RESERVE).get(0).values());
    }

    @Test
    void migrateAtSumsBothSourcesAndIsANoOpOnLegacy() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.ZSET_MIGRATE, 2L, 3L);
        assertEquals(5, zset(rec).migrateAt(1000));

        RecordingRedis legacyRec = new RecordingRedis();
        assertEquals(0, legacy(legacyRec).migrateAt(1000));
        assertTrue(legacyRec.calls.isEmpty());

        assertEquals(0, zset(new RecordingRedis()).migrateAt(1000)); // a null script reply counts as 0
    }

    // ----------------------------------------------------------------------------- legacy reserve

    @Test
    void legacyReserveSweepsThenMovesTheHeadWithAFreshToken() {
        RecordingRedis rec = new RecordingRedis()
            .script(RedisLua.LEGACY_SWEEP, 0L)
            .script(RedisLua.LEGACY_RESERVE, BODY);

        Reservation r = legacy(rec).reserve();

        assertEquals(BODY, r.body());
        assertFalse(r.poison());
        assertTrue(TOKEN.matcher(r.handle()).matches());
        assertEquals(r.handle() + ":" + BODY, r.legacyMember());

        RecordingRedis.Call sweep = rec.scriptCalls(RedisLua.LEGACY_SWEEP).get(0);
        assertArrayEquals(new String[] {"orders", "orders:processing", "orders:processing:deadlines"}, sweep.keys());
        assertEquals(List.of("1000", "-1"), sweep.values());
        RecordingRedis.Call reserve = rec.scriptCalls(RedisLua.LEGACY_RESERVE).get(0);
        assertArrayEquals(new String[] {"orders", "orders:processing", "orders:processing:deadlines"}, reserve.keys());
        assertEquals(List.of("1090", r.handle()), reserve.values());
        assertEquals(ScriptOutputType.VALUE, RecordingRedis.outputType(reserve));
        assertTrue(rec.calls.indexOf(rec.scriptCalls(RedisLua.LEGACY_SWEEP).get(0))
            < rec.calls.indexOf(reserve), "the sweep runs before the reserve");
    }

    @Test
    void legacyReserveOnAnEmptyQueueReturnsNullAndUsesPlusInfWhenRecoveryIsOff() {
        RecordingRedis rec = new RecordingRedis();
        assertNull(ops(rec, RedisLayout.LEGACY, null, -1, 1000.4).reserve());
        assertEquals("+inf", rec.scriptCalls(RedisLua.LEGACY_RESERVE).get(0).values().get(0));
        // the sweep still runs with recovery off (it recovers other workers' reservations)
        assertEquals(1, rec.scriptCalls(RedisLua.LEGACY_SWEEP).size());
    }

    @Test
    void sweepAtIsLegacyOnly() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.LEGACY_SWEEP, 4L);
        assertEquals(4, legacy(rec).sweepAt(1000));
        assertEquals(0, zset(new RecordingRedis()).sweepAt(1000));
        assertEquals(0, legacy(new RecordingRedis()).sweepAt(1000));
    }

    // ----------------------------------------------------------------------------- delivery count

    @Test
    void deliveryCountIsStoredPlusOneOnZsetAndStoredOnLegacy() {
        String stored = EnvelopeCodec.encode(EnvelopeCodec.decode(BODY).withAttempts(2));
        RecordingRedis rec = new RecordingRedis();
        assertEquals(3, zset(rec).deliveryCount(new Reservation(stored, "h", false)));
        assertEquals(1, zset(rec).deliveryCount(new Reservation(stored, stored, true)));
        assertEquals(2, legacy(rec).deliveryCount(new Reservation(stored, "tok", false)));
    }

    // ------------------------------------------------------------------------------------- ack

    @Test
    void zsetAckIsAZremOfTheReservedMember() {
        RecordingRedis rec = new RecordingRedis().zrem(1L).zrem(0L).zrem(null);
        RedisQueueOps ops = zset(rec);
        Reservation r = new Reservation(BODY, "member", false);
        assertTrue(ops.ack(r));
        assertFalse(ops.ack(r));
        assertFalse(ops.ack(r));
        RecordingRedis.Call zrem = rec.methodCalls("zrem").get(0);
        assertEquals(List.of("queues:orders:reserved", "member"), zrem.args());
    }

    @Test
    void legacyAckIsGatedByTheDeadlineMember() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.LEGACY_ACK, 1L, 0L, null);
        RedisQueueOps ops = legacy(rec);
        Reservation r = new Reservation(BODY, "0123456789abcdef0123456789abcdef", false);
        assertTrue(ops.ack(r));
        assertFalse(ops.ack(r)); // stale: touched nothing
        assertFalse(ops.ack(r));
        RecordingRedis.Call ack = rec.scriptCalls(RedisLua.LEGACY_ACK).get(0);
        assertArrayEquals(new String[] {"orders:processing", "orders:processing:deadlines"}, ack.keys());
        assertEquals(List.of(r.legacyMember(), BODY), ack.values());
    }

    // --------------------------------------------------------------------------------- release

    @Test
    void zsetReleaseSchedulesTheReEncodedBodyWithAttemptsPlusOne() {
        RecordingRedis rec = new RecordingRedis();
        Reservation r = new Reservation(BODY, "reserved-member", false);

        zset(rec).releaseAt(r, 5, 1000.3);

        RecordingRedis.Call call = rec.scriptCalls(RedisLua.ZSET_RELEASE).get(0);
        assertArrayEquals(new String[] {"queues:orders:reserved", "queues:orders:delayed"}, call.keys());
        assertEquals("reserved-member", call.values().get(0));
        assertEquals("1005", call.values().get(1));
        assertEquals(1, EnvelopeCodec.decode(call.values().get(2)).attempts());
    }

    @Test
    void releaseUsesTheInjectedClock() {
        RecordingRedis rec = new RecordingRedis();
        zset(rec).release(new Reservation(BODY, "m", false), 2);
        assertEquals("1002", rec.scriptCalls(RedisLua.ZSET_RELEASE).get(0).values().get(1));
    }

    @Test
    void releaseRejectsBadInput() {
        RecordingRedis rec = new RecordingRedis();
        Reservation poison = new Reservation("not-json", "not-json", true);
        assertThrows(IllegalArgumentException.class, () -> zset(rec).releaseAt(
            new Reservation(BODY, "m", false), -1, 1000));
        assertThrows(IllegalStateException.class, () -> zset(rec).releaseAt(poison, 1, 1000));
        assertThrows(UnsupportedOperationException.class, () -> legacy(rec).releaseAt(
            new Reservation(BODY, "tok", false), 1, 1000));
        assertTrue(rec.calls.isEmpty());
    }

    @Test
    void legacyImmediateReleaseMovesBackToTheReadyListThroughTheGate() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.LEGACY_MOVE, 1L, 0L);
        RedisQueueOps ops = legacy(rec);
        Reservation r = new Reservation(BODY, "0123456789abcdef0123456789abcdef", false);

        ops.releaseAt(r, 0, 1000);
        ops.releaseAt(r, 0, 1000); // gate false: logged, nothing thrown

        RecordingRedis.Call call = rec.scriptCalls(RedisLua.LEGACY_MOVE).get(0);
        assertArrayEquals(new String[] {"orders:processing", "orders:processing:deadlines", "orders"}, call.keys());
        assertEquals(r.legacyMember(), call.values().get(0));
        assertEquals(BODY, call.values().get(1));
        assertEquals(1, EnvelopeCodec.decode(call.values().get(2)).attempts());
    }

    @Test
    void releaseBodyOnlyChangesAttemptsAndKeepsUnknownKeys() {
        String raw = "{\"job\":\"urn:babel:orders:created\",\"trace_id\":\"t\",\"data\":{\"order_id\":1},"
            + "\"meta\":{\"id\":\"f1e2d3c4-b5a6-4789-90ab-cdef01234567\",\"queue\":\"orders\",\"lang\":\"php\","
            + "\"schema_version\":1,\"created_at\":1749132727000},\"attempts\":0,\"n\":\"x\",\"e\":{},\"a\":[]}";
        String released = RedisQueueOps.releaseBody(raw);
        Envelope env = EnvelopeCodec.decode(released);
        assertEquals(1, env.attempts());
        assertTrue(released.contains("\"n\":\"x\""), released);
        assertTrue(released.contains("\"e\":{}"), released);
        assertTrue(released.contains("\"a\":[]"), released);
    }

    @Test
    void releaseBodyReturnsANonConformantBodyUnchanged() {
        assertEquals("not-json", RedisQueueOps.releaseBody("not-json"));
        assertEquals("{\"a\":1}", RedisQueueOps.releaseBody("{\"a\":1}"));
    }

    // ----------------------------------------------------------------------------- dead-letter

    @Test
    void zsetDeadLetterWritesTheRawBodyOnlyWhenItRemovedTheReservation() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.ZSET_DEAD_LETTER, 1L, 0L, null);
        RedisQueueOps ops = zset(rec);
        Reservation r = new Reservation("raw", "member", false);
        assertTrue(ops.deadLetter(r));
        assertFalse(ops.deadLetter(r));
        assertFalse(ops.deadLetter(r));
        RecordingRedis.Call call = rec.scriptCalls(RedisLua.ZSET_DEAD_LETTER).get(0);
        assertArrayEquals(new String[] {"queues:orders:reserved", "orders.dlq"}, call.keys());
        assertEquals(List.of("member", "raw"), call.values());
    }

    @Test
    void legacyDeadLetterMovesTheRawBodyToTheDlqThroughTheGate() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.LEGACY_MOVE, 1L, 0L);
        RedisQueueOps ops = legacy(rec);
        Reservation r = new Reservation("raw", "0123456789abcdef0123456789abcdef", false);
        assertTrue(ops.deadLetter(r));
        assertFalse(ops.deadLetter(r));
        RecordingRedis.Call call = rec.scriptCalls(RedisLua.LEGACY_MOVE).get(0);
        assertArrayEquals(new String[] {"orders:processing", "orders:processing:deadlines", "orders.dlq"},
            call.keys());
        assertEquals(List.of(r.legacyMember(), "raw", "raw"), call.values());
    }

    @Test
    void zsetDeadLetterWithACustomBodyRemovesTheReservedMemberAndWritesThatBody() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.ZSET_DEAD_LETTER, 1L, 0L);
        RedisQueueOps ops = zset(rec);
        Reservation r = new Reservation("original", "member", false);
        assertTrue(ops.deadLetter(r, "annotated"));
        assertFalse(ops.deadLetter(r, "annotated"));
        RecordingRedis.Call call = rec.scriptCalls(RedisLua.ZSET_DEAD_LETTER).get(0);
        assertArrayEquals(new String[] {"queues:orders:reserved", "orders.dlq"}, call.keys());
        assertEquals(List.of("member", "annotated"), call.values());
    }

    @Test
    void legacyDeadLetterWithACustomBodyRemovesTheOriginalFromProcessingAndWritesThatBody() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.LEGACY_MOVE, 1L, 0L);
        RedisQueueOps ops = legacy(rec);
        Reservation r = new Reservation("original", "0123456789abcdef0123456789abcdef", false);
        assertTrue(ops.deadLetter(r, "annotated"));
        assertFalse(ops.deadLetter(r, "annotated"));
        RecordingRedis.Call call = rec.scriptCalls(RedisLua.LEGACY_MOVE).get(0);
        assertArrayEquals(new String[] {"orders:processing", "orders:processing:deadlines", "orders.dlq"},
            call.keys());
        // The reservation is still identified by the stored body; only the DLQ payload differs.
        assertEquals(List.of(r.legacyMember(), "original", "annotated"), call.values());
    }

    // ------------------------------------------------------------------------------- publishing

    @Test
    void pushUsesTheLaravelScriptOnZsetAndRpushOnLegacy() {
        RecordingRedis rec = new RecordingRedis();
        zset(rec).push(BODY);
        RecordingRedis.Call push = rec.scriptCalls(RedisLua.ZSET_PUSH).get(0);
        assertArrayEquals(new String[] {"queues:orders", "queues:orders:notify"}, push.keys());
        assertEquals(List.of(BODY), push.values());

        RecordingRedis legacyRec = new RecordingRedis();
        legacy(legacyRec).push(BODY);
        assertEquals(List.of("orders", BODY), legacyRec.methodCalls("rpush").get(0).args());
    }

    @Test
    void pushDelayedAddsToTheDelayedSetWithoutANotifyToken() {
        RecordingRedis rec = new RecordingRedis();
        RedisQueueOps ops = zset(rec);
        ops.pushDelayedAt(BODY, 1.5, 1000.7);
        ops.pushDelayed(BODY, 10); // injected clock 1000.4
        List<RecordingRedis.Call> zadds = rec.methodCalls("zadd");
        assertEquals(List.of("queues:orders:delayed", 1002.0, BODY), zadds.get(0).args());
        assertEquals(List.of("queues:orders:delayed", 1010.0, BODY), zadds.get(1).args());
        assertTrue(rec.methodCalls("rpush").isEmpty(), "no notify token on a delayed publish");
        assertTrue(rec.scriptCalls(RedisLua.ZSET_PUSH).isEmpty());
    }

    @Test
    void pushDelayedRejectsTheLegacyLayoutAndNegativeDelays() {
        RecordingRedis rec = new RecordingRedis();
        assertThrows(IllegalStateException.class, () -> legacy(rec).pushDelayedAt(BODY, 1, 1000));
        assertThrows(IllegalArgumentException.class, () -> zset(rec).pushDelayedAt(BODY, -1, 1000));
        assertTrue(rec.calls.isEmpty());
    }

    // ---------------------------------------------------------------------------------- wait

    @Test
    void awaitNotifyBlocksOnTheNotifyListWithAFloor() {
        RecordingRedis rec = new RecordingRedis();
        RedisQueueOps ops = zset(rec);
        ops.awaitNotify(0.5);
        ops.awaitNotify(0);
        List<RecordingRedis.Call> blpops = rec.methodCalls("blpop");
        assertEquals(List.of(0.5, "queues:orders:notify"), blpops.get(0).args());
        assertEquals(List.of(0.01, "queues:orders:notify"), blpops.get(1).args());
    }

    // ------------------------------------------------------------------------------ LuaExecutor

    @Test
    void luaExecutorFallsBackToEvalOnNoScript() {
        RecordingRedis rec = new RecordingRedis().noScriptOnce().script(RedisLua.ZSET_PUSH, 1L);
        Long result = new LuaExecutor(rec.redis).run(RedisLua.ZSET_PUSH, ScriptOutputType.INTEGER,
            new String[] {"a", "b"}, "x");
        assertEquals(1L, result);
        assertEquals(1, rec.methodCalls("evalsha").size());
        RecordingRedis.Call eval = rec.methodCalls("eval").get(0);
        assertEquals(RedisLua.ZSET_PUSH, eval.script());
    }

    @Test
    void luaExecutorUsesTheSha1DigestFirst() {
        RecordingRedis rec = new RecordingRedis().script(RedisLua.ZSET_PUSH, 1L);
        new LuaExecutor(rec.redis).run(RedisLua.ZSET_PUSH, ScriptOutputType.INTEGER, new String[] {"a", "b"}, "x");
        assertEquals(RedisLua.sha1(RedisLua.ZSET_PUSH), rec.methodCalls("evalsha").get(0).script());
        assertEquals(40, RedisLua.sha1(RedisLua.ZSET_PUSH).length());
        assertTrue(rec.methodCalls("eval").isEmpty());
    }
}
