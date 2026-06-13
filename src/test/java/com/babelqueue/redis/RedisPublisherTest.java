package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class RedisPublisherTest {

    @SuppressWarnings("unchecked")
    private static RedisCommands<String, String> mockRedis() {
        return Mockito.mock(RedisCommands.class);
    }

    @Test
    void publishRpushesTheRawEnvelopeToTheReadyList() {
        RedisCommands<String, String> redis = mockRedis();
        RedisPublisher publisher = RedisPublisher.create(redis, "orders");

        String id = publisher.publish("urn:babel:orders:created", Map.of("order_id", 1042));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<String> values = ArgumentCaptor.forClass(String.class);
        verify(redis).rpush(eq("orders"), values.capture());

        Envelope env = EnvelopeCodec.decode(values.getValue());
        assertEquals("urn:babel:orders:created", env.job());
        assertEquals("orders", env.meta().queue());
        assertEquals("java", env.meta().lang());
        assertEquals(1, env.meta().schemaVersion());
        assertEquals(0, env.attempts());
        assertEquals(id, env.meta().id());
        assertEquals(1042, ((Number) env.data().get("order_id")).intValue());
        assertNotNull(env.traceId());
    }

    @Test
    void publishContinuesAnExistingTraceId() {
        RedisCommands<String, String> redis = mockRedis();
        RedisPublisher publisher = RedisPublisher.create(redis, "orders");

        publisher.publish("urn:babel:orders:created", Map.of("x", 1), "trace-abc");

        ArgumentCaptor<String> values = ArgumentCaptor.forClass(String.class);
        verify(redis).rpush(eq("orders"), values.capture());
        assertEquals("trace-abc", EnvelopeCodec.decode(values.getValue()).traceId());
    }

    @Test
    void publishMintsAFreshTraceWhenNoneGiven() {
        RedisCommands<String, String> redis = mockRedis();
        RedisPublisher.create(redis, "orders").publish("urn:babel:orders:created", null);

        ArgumentCaptor<String> values = ArgumentCaptor.forClass(String.class);
        verify(redis).rpush(eq("orders"), values.capture());
        String trace = EnvelopeCodec.decode(values.getValue()).traceId();
        assertNotNull(trace);
        assertEquals(36, trace.length()); // a UUID
    }

    @Test
    void blankUrnIsRejectedBeforeAnyRedisCall() {
        RedisCommands<String, String> redis = mockRedis();
        assertThrows(BabelQueueException.class,
            () -> RedisPublisher.create(redis, "orders").publish("  ", Map.of()));
        verifyNoInteractions(redis);
    }

    @Test
    void clientErrorPropagates() {
        RedisCommands<String, String> redis = mockRedis();
        when(redis.rpush(eq("orders"), Mockito.<String>any()))
            .thenThrow(new RuntimeException("redis down"));
        RuntimeException e = assertThrows(RuntimeException.class,
            () -> RedisPublisher.create(redis, "orders").publish("urn:x:y", Map.of()));
        assertEquals("redis down", e.getMessage());
    }

    @Test
    void exposesItsQueueName() {
        assertEquals("orders", RedisPublisher.create(mockRedis(), "orders").queue());
    }
}
