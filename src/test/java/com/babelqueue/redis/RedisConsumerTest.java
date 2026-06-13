package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import io.lettuce.core.LMoveArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class RedisConsumerTest {

    private static final String QUEUE = "orders";
    private static final String PROCESSING = "orders:processing";

    @SuppressWarnings("unchecked")
    private static RedisCommands<String, String> mockRedis() {
        return Mockito.mock(RedisCommands.class);
    }

    private static String envelope(int attempts) {
        Envelope base = EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 7), QUEUE, null);
        Envelope bumped = new Envelope(
            base.job(), base.traceId(), base.data(), base.meta(), attempts, null);
        return EnvelopeCodec.encode(bumped);
    }

    /** Stub blmove to return {@code body} once, then null (empty queue) on later polls. */
    private static void seedOne(RedisCommands<String, String> redis, String body) {
        when(redis.blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong()))
            .thenReturn(body, (String) null);
    }

    /** The Redis wire form of an {@link LMoveArgs} (encodes the LEFT/RIGHT directions). */
    private static String commandString(LMoveArgs args) {
        CommandArgs<String, String> wire = new CommandArgs<>(StringCodec.UTF8);
        args.build(wire);
        return wire.toCommandString();
    }

    @Test
    void reservesValidMessageThenAcksWithLrem() {
        RedisCommands<String, String> redis = mockRedis();
        String body = envelope(0);
        seedOne(redis, body);

        Envelope[] seen = {null};
        String[] rawSeen = {null};
        int n = RedisConsumer.builder(redis, QUEUE)
            .handler("urn:babel:orders:created", (env, raw) -> { seen[0] = env; rawSeen[0] = raw; })
            .build()
            .poll();

        assertEquals(1, n);
        assertEquals("urn:babel:orders:created", seen[0].job());
        assertEquals(7, ((Number) seen[0].data().get("order_id")).intValue());
        assertEquals(body, rawSeen[0]); // handler sees the raw body verbatim
        verify(redis).lrem(PROCESSING, 1, body); // acked
    }

    @Test
    void reservesWithLeftToRightMove() {
        RedisCommands<String, String> redis = mockRedis();
        seedOne(redis, envelope(0));

        RedisConsumer.builder(redis, QUEUE)
            .handler("urn:babel:orders:created", (e, b) -> { })
            .build()
            .poll();

        ArgumentCaptor<LMoveArgs> args = ArgumentCaptor.forClass(LMoveArgs.class);
        ArgumentCaptor<Long> timeout = ArgumentCaptor.forClass(Long.class);
        verify(redis).blmove(eq(QUEUE), eq(PROCESSING), args.capture(), timeout.capture());
        // leftRight() == pop head of the ready list, push to the tail of processing (FIFO + crash-safe).
        // Materialize both into Redis command args and compare the wire form (which encodes LEFT/RIGHT).
        assertEquals(commandString(LMoveArgs.Builder.leftRight()), commandString(args.getValue()));
        assertEquals(5L, timeout.getValue()); // default block timeout
    }

    @Test
    void emptyReservationReturnsZeroAndDoesNotAck() {
        RedisCommands<String, String> redis = mockRedis();
        when(redis.blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong())).thenReturn(null);

        boolean[] handled = {false};
        int n = RedisConsumer.builder(redis, QUEUE)
            .handler("urn:babel:orders:created", (e, b) -> handled[0] = true)
            .build()
            .poll();

        assertEquals(0, n);
        assertTrue(!handled[0]);
        verify(redis, never()).lrem(any(), anyLong(), any());
    }

    @Test
    void attemptsAreTakenFromTheBodyUnchanged() {
        RedisCommands<String, String> redis = mockRedis();
        seedOne(redis, envelope(4)); // Redis has no native delivery count

        int[] attempts = {-1};
        RedisConsumer.builder(redis, QUEUE)
            .handler("urn:babel:orders:created", (e, b) -> attempts[0] = e.attempts())
            .build()
            .poll();
        assertEquals(4, attempts[0]);
    }

    @Test
    void throwingHandlerLeavesMessageOnProcessingAndReportsOnError() {
        RedisCommands<String, String> redis = mockRedis();
        seedOne(redis, envelope(0));

        Throwable[] captured = {null};
        RedisConsumer.builder(redis, QUEUE)
            .handler("urn:babel:orders:created", (e, b) -> { throw new IllegalStateException("boom"); })
            .onError((err, env, body) -> captured[0] = err)
            .build()
            .poll();

        assertInstanceOf(IllegalStateException.class, captured[0]);
        verify(redis, never()).lrem(any(), anyLong(), any()); // not acked — left for recovery sweep
    }

    @Test
    void nonConformantEnvelopeReportsOnErrorAndIsNotAcked() {
        RedisCommands<String, String> redis = mockRedis();
        seedOne(redis, "{\"not\":\"an envelope\"}");

        Throwable[] captured = {null};
        RedisConsumer.builder(redis, QUEUE)
            .onError((err, env, body) -> captured[0] = err)
            .build()
            .poll();

        assertInstanceOf(BabelQueueException.class, captured[0]);
        verify(redis, never()).lrem(any(), anyLong(), any());
    }

    @Test
    void unknownUrnHandlerIsCalledThenMessageIsAcked() {
        RedisCommands<String, String> redis = mockRedis();
        String body = envelope(0);
        seedOne(redis, body);

        String[] unknown = {""};
        RedisConsumer.builder(redis, QUEUE)
            .onUnknownUrn((env, raw) -> unknown[0] = EnvelopeCodec.urn(env))
            .build()
            .poll();

        assertEquals("urn:babel:orders:created", unknown[0]);
        verify(redis).lrem(PROCESSING, 1, body); // unknown-urn handled -> acked (dropped)
    }

    @Test
    void unknownUrnWithoutHandlerReportsOnErrorAndIsNotAcked() {
        RedisCommands<String, String> redis = mockRedis();
        seedOne(redis, envelope(0));

        Throwable[] captured = {null};
        RedisConsumer.builder(redis, QUEUE)
            .onError((err, env, body) -> captured[0] = err)
            .build()
            .poll();

        assertInstanceOf(UnknownUrnException.class, captured[0]);
        verify(redis, never()).lrem(any(), anyLong(), any());
    }

    @Test
    void badMessageWithoutAnOnErrorIsSwallowedSoTheLoopSurvives() {
        RedisCommands<String, String> redis = mockRedis();
        seedOne(redis, "{\"not\":\"an envelope\"}");

        // No onError, no handler: handle() must not throw.
        int n = RedisConsumer.builder(redis, QUEUE).build().poll();
        assertEquals(1, n);
        verify(redis, never()).lrem(any(), anyLong(), any());
    }

    @Test
    void blockTimeoutIsConfigurable() {
        RedisCommands<String, String> redis = mockRedis();
        when(redis.blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), eq(0L))).thenReturn(null);

        RedisConsumer.builder(redis, QUEUE).blockTimeoutSeconds(0).build().poll();
        verify(redis).blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), eq(0L));
    }

    @Test
    void handlersMapRegistersInBulk() {
        RedisCommands<String, String> redis = mockRedis();
        String body = envelope(0);
        seedOne(redis, body);

        boolean[] hit = {false};
        RedisConsumer.builder(redis, QUEUE)
            .handlers(Map.of("urn:babel:orders:created", (e, b) -> hit[0] = true))
            .build()
            .poll();
        assertTrue(hit[0]);
        verify(redis).lrem(PROCESSING, 1, body);
    }

    @Test
    void runStopsImmediatelyWhenShouldNotContinue() {
        RedisCommands<String, String> redis = mockRedis();
        RedisConsumer.builder(redis, QUEUE).build().run(() -> false);
        verify(redis, never()).blmove(any(), any(), any(LMoveArgs.class), anyLong());
    }

    @Test
    void runLoopsWhileShouldContinue() {
        RedisCommands<String, String> redis = mockRedis();
        when(redis.blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong())).thenReturn(null);

        boolean[] once = {true};
        RedisConsumer.builder(redis, QUEUE).build().run(() -> {
            if (once[0]) {
                once[0] = false;
                return true;
            }
            return false;
        });
        verify(redis, times(1)).blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong());
    }

    @Test
    void runWithoutArgsPollsUntilInterrupted() throws Exception {
        RedisCommands<String, String> redis = mockRedis();
        when(redis.blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong())).thenReturn(null);

        RedisConsumer consumer = RedisConsumer.builder(redis, QUEUE).build();
        Thread worker = new Thread(consumer::run);
        worker.start();
        // Let it spin a few times, then stop it via interruption.
        Thread.sleep(50);
        worker.interrupt();
        worker.join(1000);
        assertTrue(!worker.isAlive());
        verify(redis, Mockito.atLeastOnce()).blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong());
    }

    @Test
    void clientErrorPropagates() {
        RedisCommands<String, String> redis = mockRedis();
        when(redis.blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong()))
            .thenThrow(new RuntimeException("redis down"));
        RuntimeException e = assertThrows(RuntimeException.class,
            () -> RedisConsumer.builder(redis, QUEUE).build().poll());
        assertEquals("redis down", e.getMessage());
    }

    @Test
    void exposesQueueKeys() {
        RedisConsumer consumer = RedisConsumer.builder(mockRedis(), QUEUE).build();
        assertEquals(QUEUE, consumer.queue());
        assertEquals(PROCESSING, consumer.processingQueue());
    }
}
