package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Live Redis: a URN with no handler and no {@code onUnknownUrn} hook is dead-lettered to {@code <queue>.dlq}
 * (reason {@code unknown_urn}) and its reservation is removed atomically in both layouts - it never loops
 * back after {@code retry_after}. Same plumbing as {@link RedisLayoutConformanceTest}: {@code BQ_REDIS_URL}
 * (default {@code redis://localhost:6379}); an unreachable Redis FAILS the run when {@code BQ_REQUIRE_REDIS}
 * is set (CI) and skips the test otherwise.
 */
class RedisUnknownUrnLiveTest {

    private static final String URL = envOr("BQ_REDIS_URL", "redis://localhost:6379");
    private static final boolean REQUIRED = System.getenv("BQ_REQUIRE_REDIS") != null;
    private static final String HANDLED_URN = "urn:bq:orders:created";
    private static final String UNKNOWN_URN = "urn:bq:orders:unmapped";

    private static RedisClient client;
    private static RedisCommands<String, String> redis;
    private static String unavailable;

    @Test
    @Tag("live")
    void legacyUnknownUrnWithoutHookIsDeadLetteredAndTheReservationIsRemoved() {
        requireRedis();
        String queue = "orders-unknown-" + UUID.randomUUID().toString().substring(0, 8);
        RedisQueueOps ops = new RedisQueueOps(redis, queue, RedisLayout.LEGACY, null, 90L, -1,
            () -> System.currentTimeMillis() / 1000.0);
        try {
            Throwable[] reported = {null};
            RedisConsumer consumer = RedisConsumer.builder(redis, queue)
                .layout(RedisLayout.LEGACY)
                .blockTimeoutSeconds(2)
                .handler(HANDLED_URN, (env, raw) -> fail("the unmapped URN must not reach a handler"))
                .onError((err, env, raw) -> reported[0] = err)
                .build();
            RedisPublisher.builder(redis, queue).layout(RedisLayout.LEGACY).build()
                .publish(UNKNOWN_URN, Map.of("order_id", 1));

            assertEquals(1, consumer.poll());

            assertInstanceOf(UnknownUrnException.class, reported[0]);
            assertDeadLettered(ops);
            assertEquals(0L, redis.llen(ops.readyKey()));
            assertEquals(0L, redis.llen(ops.inFlightKey()), "the :processing entry must be removed");
            assertEquals(0L, redis.zcard(ops.deadlinesKey()), "the reservation deadline must be removed");
            assertEquals(0, consumer.poll(), "the message must not come back");
        } finally {
            cleanup(ops);
        }
    }

    @Test
    @Tag("live")
    void zsetUnknownUrnWithoutHookIsDeadLetteredAndTheReservationIsRemoved() {
        requireRedis();
        String queue = "orders-unknown-" + UUID.randomUUID().toString().substring(0, 8);
        RedisQueueOps ops = new RedisQueueOps(redis, queue, RedisLayout.ZSET, null, 90L, -1,
            () -> System.currentTimeMillis() / 1000.0);
        try {
            Throwable[] reported = {null};
            RedisConsumer consumer = RedisConsumer.builder(redis, queue)
                .layout(RedisLayout.ZSET)
                .blockTimeoutSeconds(2)
                .handler(HANDLED_URN, (env, raw) -> fail("the unmapped URN must not reach a handler"))
                .onError((err, env, raw) -> reported[0] = err)
                .build();
            RedisPublisher.builder(redis, queue).layout(RedisLayout.ZSET).build()
                .publish(UNKNOWN_URN, Map.of("order_id", 1));

            assertEquals(1, consumer.poll());

            assertInstanceOf(UnknownUrnException.class, reported[0]);
            assertDeadLettered(ops);
            assertEquals(0L, redis.llen(ops.readyKey()));
            assertEquals(0L, redis.zcard(ops.inFlightKey()), "the :reserved member must be removed");
            assertEquals(0L, redis.zcard(ops.delayedKey()));
            assertEquals(0, consumer.poll(), "the message must not come back");
        } finally {
            cleanup(ops);
        }
    }

    /** Exactly one annotated record on the DLQ, keeping the original URN and data. */
    private static void assertDeadLettered(RedisQueueOps ops) {
        List<String> dlq = redis.lrange(ops.dlqKey(), 0, -1);
        assertEquals(1, dlq.size(), "the unknown-URN message must reach <queue>.dlq exactly once");
        String body = RedisFrame.unframe(dlq.get(0)).body();
        Envelope dead = EnvelopeCodec.decode(body);
        assertEquals(UNKNOWN_URN, EnvelopeCodec.urn(dead));
        assertEquals("unknown_urn", dead.deadLetter().reason());
        assertTrue(body.contains("\"reason\":\"unknown_urn\""), body);
        assertEquals(1, ((Number) dead.data().get("order_id")).intValue());
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
            System.getLogger(RedisUnknownUrnLiveTest.class.getName()).log(System.Logger.Level.WARNING,
                "SKIPPING the live unknown-URN cases: {0}", unavailable);
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

    private static String envOr(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
