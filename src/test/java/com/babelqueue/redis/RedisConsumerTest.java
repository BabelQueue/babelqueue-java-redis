package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Consumer dispatch + wait logic over a mocked {@link RedisQueueOps} (the queue-state machine is
 * covered against a real Redis by the integration and conformance runners). No Redis, no network.
 */
class RedisConsumerTest {

    private static final String QUEUE = "orders";
    private static final String URN = "urn:babel:orders:created";

    private RedisQueueOps ops;
    private final AtomicLong nanos = new AtomicLong();
    private final List<Long> sleeps = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private static RedisCommands<String, String> mockRedis() {
        return Mockito.mock(RedisCommands.class);
    }

    @BeforeEach
    void setUp() {
        ops = Mockito.mock(RedisQueueOps.class);
        when(ops.layout()).thenReturn(RedisLayout.LEGACY);
        when(ops.inFlightKey()).thenReturn("orders:processing");
    }

    private RedisConsumer.Builder builder() {
        return RedisConsumer.builder(mockRedis(), QUEUE)
            .ops(ops)
            .waitHooks(nanos::get, ms -> {
                sleeps.add(ms);
                nanos.addAndGet(ms * 1_000_000L);
            });
    }

    private static String envelope(int attempts) {
        Envelope base = EnvelopeCodec.make(URN, Map.of("order_id", 7), QUEUE, null);
        return EnvelopeCodec.encode(new Envelope(
            base.job(), base.traceId(), base.data(), base.meta(), attempts, null));
    }

    private static Reservation legacy(String body) {
        return new Reservation(body, "0123456789abcdef0123456789abcdef", false);
    }

    @Test
    void legacyValidMessageIsHandledAndAcked() {
        String body = envelope(3);
        Reservation r = legacy(body);
        when(ops.reserve()).thenReturn(r);

        Envelope[] seen = {null};
        String[] rawSeen = {null};
        int n = builder()
            .handler(URN, (env, raw) -> { seen[0] = env; rawSeen[0] = raw; })
            .build()
            .poll();

        assertEquals(1, n);
        assertEquals(URN, seen[0].job());
        assertEquals(7, ((Number) seen[0].data().get("order_id")).intValue());
        assertEquals(body, rawSeen[0]);
        assertEquals(3, seen[0].attempts()); // legacy: attempts are the body's, unchanged
        verify(ops).ack(r);
    }

    @Test
    void zsetReportsStoredAttemptsPlusOneAndNoHeaders() {
        when(ops.layout()).thenReturn(RedisLayout.ZSET);
        String body = envelope(2);
        Reservation r = new Reservation(body, "reserved-member", false);
        when(ops.reserve()).thenReturn(r);

        int[] attempts = {-1};
        Map<?, ?>[] headers = {null};
        builder()
            .handler(URN, (env, raw, h) -> { attempts[0] = env.attempts(); headers[0] = h; })
            .build()
            .poll();

        assertEquals(3, attempts[0]);
        assertTrue(headers[0].isEmpty());
        verify(ops).ack(r);
    }

    @Test
    void zsetDoesNotUnframeAFrameLookingValue() {
        when(ops.layout()).thenReturn(RedisLayout.ZSET);
        // A zset value is always the job as stored; a "frame" there is just a non-envelope.
        String framed = RedisFrame.frame(envelope(0), Map.of("traceparent", "x"));
        Reservation r = new Reservation(framed, framed, false);
        when(ops.reserve()).thenReturn(r);

        Throwable[] captured = {null};
        builder().onError((err, env, body) -> captured[0] = err).build().poll();
        assertInstanceOf(BabelQueueException.class, captured[0]);
        verify(ops).deadLetter(r);
    }

    @Test
    void emptyReservationReturnsZeroAndDoesNotAck() {
        when(ops.reserve()).thenReturn(null);

        boolean[] handled = {false};
        int n = builder()
            .blockTimeoutSeconds(1)
            .handler(URN, (e, b) -> handled[0] = true)
            .build()
            .poll();

        assertEquals(0, n);
        assertTrue(!handled[0]);
        verify(ops, never()).ack(any());
    }

    @Test
    void legacyWaitUsesBackoffFrom100msDoublingToACapOfOneSecond() {
        when(ops.reserve()).thenReturn(null);

        builder().blockTimeoutSeconds(3).build().poll();

        // 100 + 200 + 400 + 800 + 1000 = 2500 ms, the last slice is clipped to the 500 ms left.
        assertEquals(List.of(100L, 200L, 400L, 800L, 1000L, 500L), sleeps);
        assertEquals(3_000_000_000L, nanos.get()); // never waits past block_for
    }

    @Test
    void legacyBackoffResetsAfterAMessage() {
        Reservation r = legacy(envelope(0));
        when(ops.reserve()).thenReturn(null, null, r, null, null);

        RedisConsumer consumer = builder()
            .blockTimeoutSeconds(1)
            .handler(URN, (e, b) -> { })
            .build();
        consumer.poll(); // two empty polls (100, 200) then the message
        sleeps.clear();
        nanos.set(0);
        consumer.poll(); // starts again from 100 ms
        assertEquals(List.of(100L, 200L, 400L, 300L), sleeps);
    }

    @Test
    void zsetWaitBlocksOnNotifyInSlicesOfAtMostOneSecond() {
        when(ops.layout()).thenReturn(RedisLayout.ZSET);
        when(ops.reserve()).thenReturn(null);
        List<Double> slices = new ArrayList<>();
        doAnswer(inv -> {
            double s = inv.getArgument(0);
            slices.add(s);
            nanos.addAndGet((long) (s * 1e9));
            return null;
        }).when(ops).awaitNotify(anyDouble());

        builder().blockTimeoutSeconds(3).build().poll();

        assertEquals(List.of(1.0, 1.0, 1.0), slices);
        assertTrue(sleeps.isEmpty()); // :notify is the wake-up, no sleep-poll in zset mode
    }

    @Test
    void throwingHandlerReportsOnErrorAndLeavesTheReservation() {
        Reservation r = legacy(envelope(0));
        when(ops.reserve()).thenReturn(r);

        Throwable[] captured = {null};
        builder()
            .handler(URN, (e, b) -> { throw new IllegalStateException("boom"); })
            .onError((err, env, body) -> captured[0] = err)
            .build()
            .poll();

        assertInstanceOf(IllegalStateException.class, captured[0]);
        verify(ops, never()).ack(any());
        verify(ops, never()).release(any(), Mockito.anyDouble());
    }

    @Test
    void zsetThrowingHandlerWithFailureDelayReleasesToDelayed() {
        when(ops.layout()).thenReturn(RedisLayout.ZSET);
        Reservation r = new Reservation(envelope(0), "m", false);
        when(ops.reserve()).thenReturn(r);

        builder()
            .failureDelaySeconds(30)
            .handler(URN, (e, b) -> { throw new IllegalStateException("boom"); })
            .build()
            .poll();

        verify(ops).release(r, 30.0);
        verify(ops, never()).ack(any());
    }

    @Test
    void nonConformantEnvelopeReportsOnErrorAndIsDeadLettered() {
        Reservation r = legacy("{\"not\":\"an envelope\"}");
        when(ops.reserve()).thenReturn(r);

        Throwable[] captured = {null};
        builder().onError((err, env, body) -> captured[0] = err).build().poll();

        assertInstanceOf(BabelQueueException.class, captured[0]);
        verify(ops).deadLetter(r);
        verify(ops, never()).ack(any());
    }

    @Test
    void poisonReservationIsReportedAndDeadLettered() {
        when(ops.layout()).thenReturn(RedisLayout.ZSET);
        Reservation r = new Reservation("not json", "not json", true);
        when(ops.reserve()).thenReturn(r);

        Throwable[] captured = {null};
        boolean[] handled = {false};
        builder()
            .handler(URN, (e, b) -> handled[0] = true)
            .onError((err, env, body) -> captured[0] = err)
            .build()
            .poll();

        assertInstanceOf(BabelQueueException.class, captured[0]);
        assertTrue(!handled[0]);
        verify(ops).deadLetter(r);
    }

    @Test
    void unknownUrnHandlerIsCalledThenMessageIsAcked() {
        Reservation r = legacy(envelope(0));
        when(ops.reserve()).thenReturn(r);

        String[] unknown = {""};
        builder().onUnknownUrn((env, raw) -> unknown[0] = EnvelopeCodec.urn(env)).build().poll();

        assertEquals(URN, unknown[0]);
        verify(ops).ack(r);
    }

    @Test
    void unknownUrnWithoutHandlerReportsOnErrorAndIsNotAcked() {
        when(ops.reserve()).thenReturn(legacy(envelope(0)));

        Throwable[] captured = {null};
        builder().onError((err, env, body) -> captured[0] = err).build().poll();

        assertInstanceOf(UnknownUrnException.class, captured[0]);
        verify(ops, never()).ack(any());
    }

    @Test
    void badMessageWithoutAnOnErrorIsSwallowedSoTheLoopSurvives() {
        when(ops.reserve()).thenReturn(legacy("{\"not\":\"an envelope\"}"));
        assertEquals(1, builder().build().poll());
    }

    @Test
    void legacyUnframesAndSurfacesHeaders() {
        String env = envelope(0);
        String frame = RedisFrame.frame(env, Map.of("traceparent", "00-aa-bb-01"));
        Reservation r = legacy(frame);
        when(ops.reserve()).thenReturn(r);

        String[] body = {null};
        Map<?, ?>[] headers = {null};
        builder().handler(URN, (e, b, h) -> { body[0] = b; headers[0] = h; }).build().poll();

        assertEquals(env, body[0]);
        assertEquals("00-aa-bb-01", headers[0].get("traceparent"));
        verify(ops).ack(r); // the ack handle is the reservation, whose member embeds the stored frame
    }

    @Test
    void handlersMapRegistersInBulk() {
        Reservation r = legacy(envelope(0));
        when(ops.reserve()).thenReturn(r);

        boolean[] hit = {false};
        builder().handlers(Map.of(URN, (e, b) -> hit[0] = true)).build().poll();
        assertTrue(hit[0]);
        verify(ops).ack(r);
    }

    @Test
    void runStopsImmediatelyWhenShouldNotContinue() {
        builder().build().run(() -> false);
        verify(ops, never()).reserve();
    }

    @Test
    void runLoopsWhileShouldContinue() {
        when(ops.reserve()).thenReturn(null);
        boolean[] once = {true};
        builder().blockTimeoutSeconds(1).build().run(() -> {
            if (once[0]) {
                once[0] = false;
                return true;
            }
            return false;
        });
        verify(ops, Mockito.atLeastOnce()).reserve();
    }

    @Test
    void runWithoutArgsPollsUntilInterrupted() throws Exception {
        when(ops.reserve()).thenReturn(null);
        RedisConsumer consumer = RedisConsumer.builder(mockRedis(), QUEUE).ops(ops).blockTimeoutSeconds(1).build();
        Thread worker = new Thread(consumer::run);
        worker.start();
        Thread.sleep(150);
        worker.interrupt();
        worker.join(2000);
        assertTrue(!worker.isAlive());
        verify(ops, Mockito.atLeastOnce()).reserve();
    }

    @Test
    void clientErrorPropagates() {
        when(ops.reserve()).thenThrow(new RuntimeException("redis down"));
        RuntimeException e = assertThrows(RuntimeException.class, () -> builder().build().poll());
        assertEquals("redis down", e.getMessage());
    }

    @Test
    void exposesQueueKeys() {
        RedisConsumer consumer = builder().build();
        assertEquals(QUEUE, consumer.queue());
        assertEquals("orders:processing", consumer.processingQueue());
        verify(ops, times(1)).inFlightKey();
    }

    @Test
    void failureDelayMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () -> builder().failureDelaySeconds(-1));
    }
}
