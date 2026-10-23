package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.babelqueue.Envelope;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * The Redis transport against the vendored conformance suite's {@code redis_layout} and {@code redis_delay}
 * sections (conformance 1.6.0; ADR-0033; broker-bindings §1.1, §1.6, §1.7).
 *
 * <p>The pure cases (key names, the delayed-score formula) always run. The behavioural blocks need a live
 * Redis ({@code BQ_REDIS_URL}, default {@code redis://localhost:6379}; the CI service container is
 * {@code redis:7}): when {@code BQ_REQUIRE_REDIS} is set an unreachable Redis FAILS the run, otherwise
 * the live tests are skipped (an assumption, logged). The live tests also cover the wall-clock delayed
 * window and the legacy abandoned-reservation recovery.
 */
class RedisLayoutConformanceTest {

    private static final String URL = envOr("BQ_REDIS_URL", "redis://localhost:6379");
    private static final boolean REQUIRED = System.getenv("BQ_REQUIRE_REDIS") != null;
    private static final Pattern HEX32 = Pattern.compile("[0-9a-f]{32}");
    private static final String URN = "urn:bq:orders:created";

    private static RedisClient client;
    private static RedisCommands<String, String> redis;
    private static String unavailable;

    // ------------------------------------------------------------------------------- pure cases

    @TestFactory
    Stream<DynamicTest> keyNamesMatchTheLaravelLayout() throws Exception {
        JSONArray cases = manifest().getJSONObject("redis_layout").getJSONArray("cases");
        List<DynamicTest> tests = new ArrayList<>();
        for (int i = 0; i < cases.length(); i++) {
            JSONObject c = cases.getJSONObject(i);
            tests.add(dynamicTest(c.getString("name"), RedisConformanceResults.record("redis_layout", c.getString("name"), () -> {
                RedisQueueOps ops = new RedisQueueOps(new RecordingRedis().redis, c.getString("queue"), RedisLayout.ZSET,
                    c.getString("key_prefix"), 90L, -1, () -> 0.0);
                JSONObject expect = c.getJSONObject("expect");
                assertEquals(expect.getString("ready"), ops.readyKey());
                assertEquals(expect.getString("delayed"), ops.delayedKey());
                assertEquals(expect.getString("reserved"), ops.inFlightKey());
                assertEquals(expect.getString("notify"), ops.notifyKey());
            })));
        }
        assertTrue(!tests.isEmpty(), "the manifest has no redis_layout key-name cases");
        return tests.stream();
    }

    @TestFactory
    Stream<DynamicTest> delayedScoreFormula() throws Exception {
        JSONArray cases = manifest().getJSONObject("redis_delay").getJSONArray("cases");
        List<DynamicTest> tests = new ArrayList<>();
        for (int i = 0; i < cases.length(); i++) {
            JSONObject c = cases.getJSONObject(i);
            tests.add(dynamicTest(c.getString("name"), RedisConformanceResults.record("redis_delay", c.getString("name"), () ->
                assertEquals(c.getLong("expect_score"),
                    RedisQueueOps.delayScore(c.getDouble("now"), c.getDouble("delay"))))));
        }
        assertTrue(!tests.isEmpty(), "the manifest has no redis_delay cases");
        return tests.stream();
    }

    // ------------------------------------------------------------------- behavioural (live Redis)

    private static final String[] BLOCKS = {
        "delayed_score", "migrate", "reserve", "pcall_poison", "release",
        "processing_sweep", "processing_token", "late_ack", "identical_copies", "retry_after_null",
        "lrem_count", "malformed_member", "orphan", "legacy_release_gate", "legacy_dlq_gate",
    };

    @TestFactory
    Stream<DynamicTest> behaviouralCases() throws Exception {
        JSONObject section = manifest().getJSONObject("redis_layout");
        try {
            requireRedis();
        } catch (org.opentest4j.TestAbortedException aborted) {
            // Redis unreachable and BQ_REQUIRE_REDIS unset: every behavioural case is a recorded skip, not a
            // missing one (CI sets BQ_REQUIRE_REDIS, where requireRedis() fails instead and nothing is skipped).
            for (String block : BLOCKS) {
                JSONArray cases = section.getJSONObject(block).getJSONArray("cases");
                for (int i = 0; i < cases.length(); i++) {
                    RedisConformanceResults.skip("redis_layout", block + "/" + cases.getJSONObject(i).getString("name"),
                        "live Redis unreachable and BQ_REQUIRE_REDIS is not set");
                }
            }
            throw aborted;
        }
        List<DynamicTest> tests = new ArrayList<>();
        for (String block : BLOCKS) {
            JSONArray cases = section.getJSONObject(block).getJSONArray("cases");
            assertTrue(cases.length() > 0, "empty behavioural block " + block);
            for (int i = 0; i < cases.length(); i++) {
                JSONObject c = cases.getJSONObject(i);
                String id = block + "/" + c.getString("name");
                tests.add(dynamicTest(id, RedisConformanceResults.record("redis_layout", id, () -> runCase(c))));
            }
        }
        return tests.stream();
    }

    private void runCase(JSONObject c) {
        String queue = c.getString("queue") + "-" + UUID.randomUUID().toString().substring(0, 8);
        RedisLayout layout = RedisLayout.valueOf(c.getString("layout").toUpperCase());
        Long retryAfter = c.isNull("retry_after") ? null : c.getLong("retry_after");
        long batch = c.has("migration_batch_size") ? c.getLong("migration_batch_size") : -1;
        double[] clock = {0};
        RedisQueueOps ops = new RedisQueueOps(redis, queue, layout, null, retryAfter, batch, () -> clock[0]);
        try {
            seed(ops, c.getJSONObject("state"));
            Map<String, Reservation> reservations = new HashMap<>();
            JSONArray steps = c.getJSONArray("steps");
            for (int i = 0; i < steps.length(); i++) {
                JSONObject step = steps.getJSONObject(i);
                String where = c.getString("name") + " step " + i + " (" + step.getString("op") + ")";
                double now = step.getDouble("now");
                clock[0] = now;
                Reservation reserved = null;
                boolean reserveStep = false;
                switch (step.getString("op")) {
                    case "push_delayed" -> ops.pushDelayedAt(step.getString("body"), step.getDouble("delay"), now);
                    case "migrate" -> ops.migrateAt(now);
                    case "sweep" -> ops.sweepAt(now);
                    case "reserve" -> {
                        reserveStep = true;
                        reserved = ops.reserveAt(now);
                        if (step.has("as")) {
                            reservations.put(step.getString("as"), reserved);
                        }
                    }
                    case "release" -> ops.releaseAt(reservation(step, reservations), step.getDouble("delay"), now);
                    case "ack" -> ops.ack(reservation(step, reservations));
                    case "dead_letter" -> ops.deadLetter(reservation(step, reservations));
                    default -> fail(where + ": unknown op " + step.getString("op"));
                }
                if (step.has("expect")) {
                    expect(ops, step.getJSONObject("expect"), reserveStep, reserved, where);
                }
            }
        } finally {
            cleanup(ops);
        }
    }

    private static Reservation reservation(JSONObject step, Map<String, Reservation> reservations) {
        Object ref = step.get("reservation");
        if (ref instanceof JSONObject literal) {
            return new Reservation(literal.getString("raw"), literal.getString("token"), false);
        }
        Reservation found = reservations.get((String) ref);
        assertNotNull(found, "no reservation stored under " + ref);
        return found;
    }

    // ------------------------------------------------------------------------------------ state

    private static void seed(RedisQueueOps ops, JSONObject state) {
        boolean zset = ops.layout() == RedisLayout.ZSET;
        if (state.has("ready")) {
            for (Object m : state.getJSONArray("ready")) {
                redis.rpush(ops.readyKey(), (String) m);
            }
        }
        if (state.has("processing")) {
            for (Object m : state.getJSONArray("processing")) {
                redis.rpush(ops.inFlightKey(), (String) m);
            }
        }
        if (state.has("delayed")) {
            zadd(ops.delayedKey(), state.getJSONArray("delayed"));
        }
        if (state.has("reserved")) {
            zadd(ops.inFlightKey(), state.getJSONArray("reserved"));
        }
        if (state.has("deadlines")) {
            JSONArray deadlines = state.getJSONArray("deadlines");
            for (int i = 0; i < deadlines.length(); i++) {
                JSONObject d = deadlines.getJSONObject(i);
                redis.zadd(ops.deadlinesKey(), score(d.get("score")), deadlineMember(d));
            }
        }
        if (state.has("ready_generate")) {
            JSONObject g = state.getJSONObject("ready_generate");
            for (int i = 0; i < g.getInt("count"); i++) {
                redis.rpush(ops.readyKey(), String.format(g.getString("member"), i));
            }
        }
        if (state.has("delayed_generate")) {
            JSONObject g = state.getJSONObject("delayed_generate");
            for (int i = 0; i < g.getInt("count"); i++) {
                redis.zadd(ops.delayedKey(), score(g.get("score")), String.format(g.getString("member"), i));
            }
        }
        if (zset && state.has("notify")) {
            for (int i = 0; i < state.getInt("notify"); i++) {
                redis.rpush(ops.notifyKey(), "1");
            }
        }
    }

    private static void zadd(String key, JSONArray items) {
        for (int i = 0; i < items.length(); i++) {
            JSONObject item = items.getJSONObject(i);
            redis.zadd(key, score(item.get("score")), memberOf(item));
        }
    }

    private static String memberOf(JSONObject item) {
        if (item.has("member")) {
            return item.getString("member");
        }
        if (item.has("member_json")) {
            return item.get("member_json").toString();
        }
        return item.get("body_json").toString();
    }

    private static String deadlineMember(JSONObject d) {
        return d.has("member") ? d.getString("member") : d.getString("token") + ":" + d.getString("raw");
    }

    private static double score(Object value) {
        if (value instanceof String s) {
            assertEquals("+inf", s, "unsupported score string");
            return Double.POSITIVE_INFINITY;
        }
        return ((Number) value).doubleValue();
    }

    // ---------------------------------------------------------------------------------- expect

    private static void expect(
        RedisQueueOps ops, JSONObject expect, boolean reserveStep, Reservation reserved, String where) {
        if (expect.has("outcome")) {
            assertTrue(reserveStep, where + ": outcome only applies to a reserve step");
            String outcome = reserved == null ? "empty" : reserved.poison() ? "poison" : "delivered";
            assertEquals(expect.getString("outcome"), outcome, where + ": outcome");
        }
        if (expect.has("handler_body")) {
            assertNotNull(reserved, where + ": handler_body expects a delivery");
            assertEquals(expect.getString("handler_body"), reserved.body(), where + ": handler_body");
        }
        if (expect.has("reported_attempts")) {
            assertNotNull(reserved, where + ": reported_attempts expects a delivery");
            assertEquals(expect.getInt("reported_attempts"), ops.deliveryCount(reserved), where + ": reported_attempts");
        }
        if (expect.has("ready")) {
            assertEquals(strings(expect.getJSONArray("ready")), redis.lrange(ops.readyKey(), 0, -1), where + ": ready");
        }
        if (expect.has("ready_generate")) {
            JSONObject g = expect.getJSONObject("ready_generate");
            List<String> generated = new ArrayList<>();
            for (int i = 0; i < g.getInt("count"); i++) {
                generated.add(String.format(g.getString("member"), i));
            }
            assertEquals(generated, redis.lrange(ops.readyKey(), 0, -1), where + ": ready_generate");
        }
        if (expect.has("ready_count")) {
            assertEquals(expect.getLong("ready_count"), redis.llen(ops.readyKey()), where + ": ready_count");
        }
        if (expect.has("dlq_count")) {
            assertEquals(expect.getLong("dlq_count"), redis.llen(ops.dlqKey()), where + ": dlq_count");
        }
        if (expect.has("processing")) {
            assertEquals(strings(expect.getJSONArray("processing")), redis.lrange(ops.inFlightKey(), 0, -1),
                where + ": processing");
        }
        if (expect.has("notify")) {
            assertEquals(expect.getLong("notify"), redis.llen(ops.notifyKey()), where + ": notify");
        }
        if (expect.has("delayed")) {
            scored(where + ": delayed", expect.getJSONArray("delayed"), redis.zrangeWithScores(ops.delayedKey(), 0, -1));
        }
        if (expect.has("reserved")) {
            scored(where + ": reserved", expect.getJSONArray("reserved"), redis.zrangeWithScores(ops.inFlightKey(), 0, -1));
        }
        if (expect.has("deadlines")) {
            deadlines(where + ": deadlines", expect, redis.zrangeWithScores(ops.deadlinesKey(), 0, -1));
        }
    }

    private static List<String> strings(JSONArray array) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            out.add(array.getString(i));
        }
        return out;
    }

    /** Order-insensitive match of {@code member|member_json|body_json + score} items. */
    private static void scored(String where, JSONArray expected, List<ScoredValue<String>> actual) {
        assertEquals(expected.length(), actual.size(), where + ": size, actual=" + actual);
        List<ScoredValue<String>> remaining = new ArrayList<>(actual);
        for (int i = 0; i < expected.length(); i++) {
            JSONObject item = expected.getJSONObject(i);
            ScoredValue<String> hit = null;
            for (ScoredValue<String> candidate : remaining) {
                if (candidate.getScore() == score(item.get("score")) && sameMember(item, candidate.getValue())) {
                    hit = candidate;
                    break;
                }
            }
            if (hit == null) {
                fail(where + ": no member matches " + item + " in " + actual);
            }
            remaining.remove(hit);
        }
    }

    private static boolean sameMember(JSONObject item, String actual) {
        if (item.has("member")) {
            return item.getString("member").equals(actual);
        }
        Object expectedJson = item.has("member_json") ? item.get("member_json") : item.get("body_json");
        try {
            return new JSONObject(expectedJson.toString()).similar(new JSONObject(actual));
        } catch (RuntimeException notJson) {
            return false;
        }
    }

    /** Deadline entries: {@code {token, raw, score}} (token {@code <any>} = 32 hex) or {@code {member, score}}. */
    private static void deadlines(String where, JSONObject expect, List<ScoredValue<String>> actual) {
        JSONArray expected = expect.getJSONArray("deadlines");
        assertEquals(expected.length(), actual.size(), where + ": size, actual=" + actual);
        List<ScoredValue<String>> remaining = new ArrayList<>(actual);
        Set<String> tokens = new HashSet<>();
        for (int i = 0; i < expected.length(); i++) {
            JSONObject item = expected.getJSONObject(i);
            ScoredValue<String> hit = null;
            for (ScoredValue<String> candidate : remaining) {
                if (candidate.getScore() == score(item.get("score")) && deadlineMatches(item, candidate.getValue())) {
                    hit = candidate;
                    break;
                }
            }
            if (hit == null) {
                fail(where + ": no deadline matches " + item + " in " + actual);
            }
            remaining.remove(hit);
            if (hit.getValue().length() > 33 && hit.getValue().charAt(32) == ':') {
                tokens.add(hit.getValue().substring(0, 32));
            }
        }
        if (expect.optBoolean("tokens_distinct", false)) {
            assertEquals(actual.size(), tokens.size(), where + ": tokens_distinct");
        }
    }

    private static boolean deadlineMatches(JSONObject item, String member) {
        if (item.has("member")) {
            return item.getString("member").equals(member);
        }
        if (member.length() < 34 || member.charAt(32) != ':') {
            return false;
        }
        String token = member.substring(0, 32);
        String raw = member.substring(33);
        String wanted = item.getString("token");
        boolean tokenOk = wanted.equals("<any>") ? HEX32.matcher(token).matches() : wanted.equals(token);
        return tokenOk && item.getString("raw").equals(raw);
    }

    // ------------------------------------------------------------------------- live scenarios

    /** A delayed publish (delay 2 s) becomes deliverable within the contract's 1.0 - 3.5 s window. */
    @Test
    @Tag("live")
    void delayedPublishArrivesInsideTheWindow() {
        requireRedis();
        String queue = "orders-live-" + UUID.randomUUID().toString().substring(0, 8);
        RedisQueueOps keys = new RedisQueueOps(redis, queue, RedisLayout.ZSET, null, 90L, -1, RedisQueueOpsClock.WALL);
        try {
            long[] handledAt = {0};
            RedisConsumer consumer = RedisConsumer.builder(redis, queue)
                .layout(RedisLayout.ZSET)
                .blockTimeoutSeconds(6)
                .handler(URN, (env, raw) -> { handledAt[0] = System.nanoTime(); })
                .build();
            long publishedAt = System.nanoTime();
            RedisPublisher.builder(redis, queue).layout(RedisLayout.ZSET).build()
                .publishDelayed(URN, Map.of("order_id", 1), 2);
            assertEquals(0L, redis.llen(keys.readyKey()), "a delayed message must not be ready yet");
            assertEquals(1, consumer.poll(), "the delayed message was never delivered");
            double elapsed = (handledAt[0] - publishedAt) / 1e9;
            assertTrue(elapsed >= 1.0 && elapsed <= 3.5, "delivered after " + elapsed + " s, outside [1.0, 3.5]");
        } finally {
            cleanup(keys);
        }
    }

    /**
     * A worker killed mid-message (modelled as a reservation that is never acked) is recovered by the next
     * consumer: the identical bytes, {@code attempts} unchanged. A reservation made with recovery disabled
     * (no deadline, {@code +inf}) is never swept back.
     */
    @Test
    @Tag("live")
    void legacyAbandonedReservationIsRecoveredByteForByte() throws Exception {
        requireRedis();
        String queue = "orders-kill-" + UUID.randomUUID().toString().substring(0, 8);
        String stuck = queue + "-stuck";
        RedisQueueOps dying = new RedisQueueOps(redis, queue, RedisLayout.LEGACY, null, 1L, -1, RedisQueueOpsClock.WALL);
        RedisQueueOps noDeadline = new RedisQueueOps(redis, stuck, RedisLayout.LEGACY, null, null, -1, RedisQueueOpsClock.WALL);
        try {
            String raw = "{\"job\": \"" + URN + "\",  \"attempts\": 2, \"data\": {\"id\": 1}}";
            redis.rpush(queue, raw);
            redis.rpush(stuck, raw);
            assertEquals(raw, dying.reserve().body());
            assertEquals(raw, noDeadline.reserve().body());
            assertEquals(0L, redis.llen(queue), "the reserved message left the ready list");

            Thread.sleep(2300); // the 1 s deadline (floor(now) + 1) is now certainly expired

            Envelope[] seen = new Envelope[1];
            String[] seenRaw = new String[1];
            RedisConsumer consumer = RedisConsumer.builder(redis, queue)
                .layout(RedisLayout.LEGACY)
                .blockTimeoutSeconds(2)
                .retryAfterSeconds(1)
                .handler(URN, (env, body) -> { seen[0] = env; seenRaw[0] = body; })
                .build();
            assertEquals(1, consumer.poll(), "the abandoned message was not recovered");
            assertEquals(raw, seenRaw[0], "the recovered message must be byte-identical");
            assertEquals(2, seen[0].attempts(), "a recovery must not touch attempts");

            assertEquals(0L, noDeadline.sweepAt(noDeadline.now()), "a deadline-less reservation is never swept");
            assertEquals(List.of(raw), redis.lrange(noDeadline.inFlightKey(), 0, -1));
            assertNull(redis.lpop(stuck), "the deadline-less message must stay in :processing, not return to ready");
        } finally {
            cleanup(dying);
            cleanup(noDeadline);
        }
    }

    /**
     * A body that lua-cjson DECODES but cannot ENCODE ({@code 1e400} becomes {@code inf}) must not make the zset
     * reserve script fail after its {@code LPOP}: it is reported as poison with the raw member stored in
     * {@code :reserved}, then dead-lettered to {@code <queue>.dlq} - never lost, never stuck.
     */
    @Test
    @Tag("live")
    void zsetReserveTreatsAnUnencodableBodyAsPoisonInsteadOfLosingIt() {
        requireRedis();
        String queue = "orders-encode-" + UUID.randomUUID().toString().substring(0, 8);
        RedisQueueOps ops = new RedisQueueOps(redis, queue, RedisLayout.ZSET, null, 90L, -1, RedisQueueOpsClock.WALL);
        try {
            String raw = "{\"job\":\"" + URN + "\",\"attempts\":0,\"data\":{\"x\":1e400}}";
            redis.rpush(ops.readyKey(), raw);

            Reservation reserved = ops.reserve();

            assertNotNull(reserved, "the head was lost: the reserve script failed after the LPOP");
            assertTrue(reserved.poison(), "an unencodable body must be reported as poison");
            assertEquals(raw, reserved.body(), "the poison body must be handed back byte for byte");
            assertEquals(0L, redis.llen(ops.readyKey()));
            assertEquals(List.of(raw), redis.zrange(ops.inFlightKey(), 0, -1),
                "the raw member must be stored in :reserved");

            ops.deadLetter(reserved);

            assertEquals(List.of(raw), redis.lrange(ops.dlqKey(), 0, -1), "the poison body must reach the DLQ");
            assertEquals(0L, redis.zcard(ops.inFlightKey()), "the reservation must be gone after the dead-letter");
        } finally {
            cleanup(ops);
        }
    }

    // ------------------------------------------------------------------------------- plumbing

    /** Wall clock in fractional epoch seconds. */
    private static final class RedisQueueOpsClock {
        static final java.util.function.DoubleSupplier WALL = () -> System.currentTimeMillis() / 1000.0;
    }

    private static synchronized void requireRedis() {
        if (redis == null && unavailable == null) {
            try {
                client = RedisClient.create(URL);
                StatefulRedisConnection<String, String> connection = client.connect();
                redis = connection.sync();
                redis.ping();
            } catch (RuntimeException e) {
                redis = null;
                unavailable = "Redis at " + URL + " is unreachable: " + e.getMessage();
            }
        }
        if (redis == null) {
            if (REQUIRED) {
                fail("BQ_REQUIRE_REDIS is set but " + unavailable);
            }
            System.getLogger(RedisLayoutConformanceTest.class.getName()).log(System.Logger.Level.WARNING,
                "SKIPPING the live Redis conformance cases: {0}", unavailable);
            Assumptions.abort(unavailable + " (set BQ_REQUIRE_REDIS to fail instead of skip)");
        }
    }

    private static void cleanup(RedisQueueOps ops) {
        if (redis == null) {
            return;
        }
        redis.del(ops.readyKey(), ops.inFlightKey(), ops.delayedKey(), ops.notifyKey(), ops.deadlinesKey(), ops.dlqKey());
    }

    @AfterAll
    static void closeRedis() {
        if (client != null) {
            client.shutdown();
        }
        client = null;
        redis = null;
        unavailable = null;
    }

    private static JSONObject manifest() throws Exception {
        try (InputStream in = RedisLayoutConformanceTest.class.getResourceAsStream("/conformance/manifest.json")) {
            if (in == null) {
                throw new IllegalStateException("vendored conformance manifest missing");
            }
            return new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static String envOr(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
