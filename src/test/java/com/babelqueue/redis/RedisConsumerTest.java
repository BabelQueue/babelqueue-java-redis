package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.JsonValues;
import com.babelqueue.UnknownUrnException;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
        // The frame-looking value is a non-envelope object on the zset layout: spliced verbatim, not unwrapped.
        Map<?, ?> record = poisonRecordOf(r);
        assertTrue(record.containsKey("__bq_frame"));
        assertEquals("poison", deadLetterBlock(record).get("reason"));
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

    // ---- zset handler failure: delay = failureDelaySeconds > retry backoff > leave reservation ----------------------

    private Reservation zsetReservation(int storedAttempts) {
        when(ops.layout()).thenReturn(RedisLayout.ZSET);
        Reservation r = new Reservation(envelope(storedAttempts), "m", false);
        when(ops.reserve()).thenReturn(r);
        return r;
    }

    private static BabelHandler boom() {
        return (e, b) -> { throw new IllegalStateException("boom"); };
    }

    @Test
    void zsetThrowingHandlerWithoutPolicyLeavesTheReservationUntilRetryAfter() {
        zsetReservation(0);

        builder().handler(URN, boom()).build().poll();

        verify(ops, never()).release(any(), Mockito.anyDouble());
        verify(ops, never()).ack(any());
    }

    @Test
    void zeroScalarBackoffBaseCountsAsNoPolicyAndLeavesTheReservation() {
        zsetReservation(0);

        builder().retryBackoff(0.0).retryBackoffMultiplier(2.0).handler(URN, boom()).build().poll();

        verify(ops, never()).release(any(), Mockito.anyDouble());
    }

    @Test
    void explicitZeroFailureDelayReleasesImmediately() {
        Reservation r = zsetReservation(0);

        builder().failureDelaySeconds(0).handler(URN, boom()).build().poll();

        verify(ops).release(r, 0.0);
    }

    @Test
    void infiniteOrHugePolicyDelayIsClampedToTheCeiling() {
        for (double huge : new double[] {Double.POSITIVE_INFINITY, Double.MAX_VALUE, 1e19}) {
            setUp();
            Reservation r = zsetReservation(0);
            builder().retryBackoff((int n) -> huge).handler(URN, boom()).build().poll();
            verify(ops).release(r, RedisConsumer.MAX_FAILURE_DELAY_SECONDS);
        }
    }

    @Test
    void hugeFailureDelayIsClampedToTheCeiling() {
        Reservation r = zsetReservation(0);

        builder().failureDelaySeconds(1e19).handler(URN, boom()).build().poll();

        verify(ops).release(r, RedisConsumer.MAX_FAILURE_DELAY_SECONDS);
    }

    @Test
    void zsetThrowingHandlerUsesTheRetryBackoffPolicyByDeliveryNumber() {
        // stored attempts 2 -> the handler sees attempts 3 -> n = 3
        Reservation r = zsetReservation(2);
        List<Integer> seen = new ArrayList<>();

        builder()
            .retryBackoff((int n) -> {
                seen.add(n);
                return 10.0 * n;
            })
            .handler(URN, boom())
            .build()
            .poll();

        assertEquals(List.of(3), seen);
        verify(ops).release(r, 30.0);
    }

    @Test
    void explicitFailureDelayWinsOverTheRetryBackoffPolicy() {
        Reservation r = zsetReservation(0);

        builder()
            .failureDelaySeconds(5)
            .retryBackoff((int n) -> 99.0)
            .handler(URN, boom())
            .build()
            .poll();

        verify(ops).release(r, 5.0);
    }

    @Test
    void invalidPolicyResultCountsAsZeroDelay() {
        Reservation r = zsetReservation(0);

        builder().retryBackoff((int n) -> Double.NaN).handler(URN, boom()).build().poll();

        verify(ops).release(r, 0.0);
    }

    @Test
    void scalarBackoffKnobsGrowExponentiallyAndCapAtTheMax() {
        for (int stored = 0; stored < 4; stored++) {
            setUp();
            Reservation r = zsetReservation(stored);
            builder()
                .retryBackoff(2.0)
                .retryBackoffMultiplier(3.0)
                .retryBackoffMax(20.0)
                .handler(URN, boom())
                .build()
                .poll();
            // n = 1..4 -> 2, 6, 18, min(54, 20)
            verify(ops).release(r, new double[] {2.0, 6.0, 18.0, 20.0}[stored]);
        }
    }

    @Test
    void fullJitterScalesTheDelayByTheRandomSource() {
        Reservation r = zsetReservation(1); // n = 2 -> d = 4 * 2 = 8

        builder()
            .retryBackoff(4.0)
            .retryBackoffMultiplier(2.0)
            .retryJitter(" Full ")
            .randomSource(() -> 0.25)
            .handler(URN, boom())
            .build()
            .poll();

        verify(ops).release(r, 2.0);
    }

    @Test
    void customPolicyOverridesTheScalarKnobs() {
        Reservation r = zsetReservation(0);

        builder().retryBackoff(100.0).retryBackoff((int n) -> 7.0).handler(URN, boom()).build().poll();

        verify(ops).release(r, 7.0);
    }

    @Test
    void legacyThrowingHandlerIgnoresRetryBackoffAndLeavesTheReservation() {
        Reservation r = legacy(envelope(0));
        when(ops.reserve()).thenReturn(r);

        builder().retryBackoff(30.0).handler(URN, boom()).build().poll();

        verify(ops, never()).release(any(), anyDouble());
        verify(ops, never()).ack(any());
    }

    @Test
    void retryBackoffBuilderValidation() {
        assertThrows(IllegalArgumentException.class, () -> RedisConsumer.builder(mockRedis(), QUEUE).retryBackoff(-1.0));
        assertThrows(IllegalArgumentException.class, () -> RedisConsumer.builder(mockRedis(), QUEUE).retryBackoff(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> RedisConsumer.builder(mockRedis(), QUEUE).retryBackoffMultiplier(0.5));
        assertThrows(IllegalArgumentException.class, () -> RedisConsumer.builder(mockRedis(), QUEUE).retryBackoffMax(-1.0));
        assertThrows(IllegalArgumentException.class, () -> RedisConsumer.builder(mockRedis(), QUEUE).retryJitter("equal"));
        assertThrows(NullPointerException.class,
            () -> RedisConsumer.builder(mockRedis(), QUEUE).retryBackoff((java.util.function.IntToDoubleFunction) null));
    }

    @Test
    void nonConformantEnvelopeReportsOnErrorAndIsDeadLettered() {
        Reservation r = legacy("{\"not\":\"an envelope\"}");
        when(ops.reserve()).thenReturn(r);

        Throwable[] captured = {null};
        builder().onError((err, env, body) -> captured[0] = err).build().poll();

        assertInstanceOf(BabelQueueException.class, captured[0]);
        verify(ops, never()).ack(any());

        // ADR-0035: the original object plus the additive dead_letter block, spliced after the original members.
        ArgumentCaptor<String> written = ArgumentCaptor.forClass(String.class);
        verify(ops).deadLetter(eq(r), written.capture());
        assertTrue(written.getValue().startsWith("{\"not\":\"an envelope\",\"dead_letter\":{"), written.getValue());
        Map<?, ?> record = (Map<?, ?>) JsonValues.decode(written.getValue());
        assertEquals("an envelope", record.get("not"));
        Map<?, ?> block = deadLetterBlock(record);
        assertEquals("poison", block.get("reason"));
        assertEquals(QUEUE, block.get("original_queue"));
        assertEquals(0, ((Number) block.get("attempts")).intValue());
        assertEquals("java", block.get("lang"));
    }

    @Test
    void framedNonConformantValueIsDeadLetteredUnwrappedInTheAdrShape() {
        String framed = RedisFrame.frame("{\"not\":\"an envelope\"}", Map.of("traceparent", "x"));
        Reservation r = new Reservation(framed, framed, false);
        when(ops.reserve()).thenReturn(r);

        builder().build().poll();

        Map<?, ?> record = poisonRecordOf(r);
        assertEquals("an envelope", record.get("not"));
        assertFalse(record.containsKey("__bq_frame"), "the DLQ element is the ADR record, not a header frame");
        assertEquals("poison", deadLetterBlock(record).get("reason"));
    }

    @Test
    void nonObjectPoisonIsDeadLetteredInTheRawWrapper() {
        when(ops.layout()).thenReturn(RedisLayout.ZSET);
        Reservation r = new Reservation("not json", "not json", true);
        when(ops.reserve()).thenReturn(r);

        builder().build().poll();

        Map<?, ?> record = poisonRecordOf(r);
        assertEquals("not json", record.get("raw"));
        assertFalse(record.containsKey("raw_encoding"));
        Map<?, ?> block = deadLetterBlock(record);
        assertEquals("poison", block.get("reason"));
        assertEquals(QUEUE, block.get("original_queue"));
    }

    @Test
    void nonUtf8PoisonIsDeadLetteredAsABase64WrapperWithoutReplacementCharacters() {
        for (RedisLayout layout : RedisLayout.values()) {
            RedisQueueOps layoutOps = Mockito.mock(RedisQueueOps.class);
            when(layoutOps.layout()).thenReturn(layout);
            when(layoutOps.inFlightKey()).thenReturn("orders:processing");
            byte[] bad = {'{', '"', 'a', '"', ':', '"', (byte) 0xFF, '"', '}'};
            String lossy = new String(bad, java.nio.charset.StandardCharsets.UTF_8); // contains U+FFFD
            Reservation r = new Reservation(lossy, lossy, layout == RedisLayout.ZSET, bad, layout == RedisLayout.ZSET ? bad : null);
            when(layoutOps.reserve()).thenReturn(r);

            RedisConsumer.builder(mockRedis(), QUEUE).ops(layoutOps).build().poll();

            ArgumentCaptor<String> written = ArgumentCaptor.forClass(String.class);
            verify(layoutOps).deadLetter(eq(r), written.capture());
            assertFalse(written.getValue().contains("\uFFFD"), "no U+FFFD substitution (ADR-0035): " + layout);
            Map<?, ?> record = (Map<?, ?>) JsonValues.decode(written.getValue());
            assertEquals("base64", record.get("raw_encoding"), layout.toString());
            assertEquals(java.util.Base64.getEncoder().encodeToString(bad), record.get("raw"));
            assertArrayEquals(bad, java.util.Base64.getDecoder().decode((String) record.get("raw")));
            Map<?, ?> block = deadLetterBlock(record);
            assertEquals("poison", block.get("reason"));
            assertEquals(QUEUE, block.get("original_queue"));
            verify(layoutOps, never()).ack(any());
        }
    }

    private Map<?, ?> poisonRecordOf(Reservation r) {
        ArgumentCaptor<String> written = ArgumentCaptor.forClass(String.class);
        verify(ops).deadLetter(eq(r), written.capture());
        return (Map<?, ?>) JsonValues.decode(written.getValue());
    }

    private static Map<?, ?> deadLetterBlock(Map<?, ?> record) {
        return (Map<?, ?>) record.get("dead_letter");
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
        Map<?, ?> record = poisonRecordOf(r);
        assertEquals("not json", record.get("raw"));
        assertEquals("poison", deadLetterBlock(record).get("reason"));
    }

    /** A conformant, decodable envelope whose data carries one 0xFF byte (not valid UTF-8). */
    private static byte[] badUtf8Envelope() {
        Envelope base = EnvelopeCodec.make(URN, Map.of("note", "XX"), QUEUE, null);
        String text = EnvelopeCodec.encode(base);
        byte[] bytes = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        bytes[text.indexOf("XX")] = (byte) 0xFF;
        return bytes;
    }

    @Test
    void aDecodableEnvelopeWithInvalidUtf8IsPoisonNeverDeliveredAndDeadLetteredInTheBase64Wrapper() {
        for (RedisLayout layout : RedisLayout.values()) {
            Mockito.reset(ops);
            when(ops.layout()).thenReturn(layout);
            byte[] stored = badUtf8Envelope();
            // The text view is lossy (U+FFFD) and would decode as a valid envelope: the byte copy marks it poison.
            Reservation r = new Reservation(new String(stored, java.nio.charset.StandardCharsets.UTF_8),
                layout == RedisLayout.ZSET ? "reserved-member" : "0123456789abcdef0123456789abcdef",
                false, stored, null);
            when(ops.reserve()).thenReturn(r);

            boolean[] called = {false};
            builder().handler(URN, (env, raw) -> called[0] = true).build().poll();

            assertFalse(called[0], layout.toString());
            verify(ops, never()).ack(any());
            verify(ops, never()).release(any(), anyDouble());
            verify(ops, never()).releaseAt(any(), anyDouble(), anyDouble());
            ArgumentCaptor<String> record = ArgumentCaptor.forClass(String.class);
            verify(ops).deadLetter(eq(r), record.capture());
            Map<?, ?> decoded = (Map<?, ?>) JsonValues.decode(record.getValue());
            assertEquals("base64", decoded.get("raw_encoding"), layout.toString());
            assertArrayEquals(stored, java.util.Base64.getDecoder().decode((String) decoded.get("raw")), layout.toString());
            assertEquals("poison", deadLetterBlock(decoded).get("reason"));
        }
    }

    @Test
    void unknownUrnHandlerIsCalledThenMessageIsAcked() {
        Reservation r = legacy(envelope(0));
        when(ops.reserve()).thenReturn(r);

        String[] unknown = {""};
        builder().onUnknownUrn((env, raw) -> unknown[0] = EnvelopeCodec.urn(env)).build().poll();

        assertEquals(URN, unknown[0]);
        verify(ops).ack(r);
        verify(ops, never()).deadLetter(any(), anyString());
    }

    @Test
    void unknownUrnWithoutHookReportsOnErrorAndIsDeadLetteredWithoutAck() {
        Reservation r = legacy(envelope(2));
        when(ops.reserve()).thenReturn(r);

        Throwable[] captured = {null};
        builder().onError((err, env, body) -> captured[0] = err).build().poll();

        assertInstanceOf(UnknownUrnException.class, captured[0]);
        ArgumentCaptor<String> record = ArgumentCaptor.forClass(String.class);
        verify(ops).deadLetter(eq(r), record.capture());
        verify(ops, never()).ack(any());
        verify(ops, never()).release(any(), anyDouble());

        Envelope dead = EnvelopeCodec.decode(record.getValue());
        assertEquals(URN, EnvelopeCodec.urn(dead));
        assertEquals("unknown_urn", dead.deadLetter().reason());
        assertEquals(QUEUE, dead.deadLetter().originalQueue());
        assertEquals(2, dead.deadLetter().attempts());
        assertEquals(7, ((Number) dead.data().get("order_id")).intValue());
    }

    @Test
    void zsetUnknownUrnDeadLettersABareAnnotatedRecordWithAttemptsPlusOne() {
        when(ops.layout()).thenReturn(RedisLayout.ZSET);
        Reservation r = new Reservation(envelope(2), "reserved-member", false);
        when(ops.reserve()).thenReturn(r);

        builder().build().poll();

        ArgumentCaptor<String> record = ArgumentCaptor.forClass(String.class);
        verify(ops).deadLetter(eq(r), record.capture());
        verify(ops, never()).ack(any());
        assertFalse(record.getValue().contains("__bq_frame"));
        Envelope dead = EnvelopeCodec.decode(record.getValue());
        assertEquals("unknown_urn", dead.deadLetter().reason());
        assertEquals(3, dead.deadLetter().attempts()); // stored 2 + 1 (the delivery the consumer saw)
    }

    @Test
    void legacyUnknownUrnKeepsTheHeaderFrameSoTraceparentSurvives() {
        String frame = RedisFrame.frame(envelope(0), Map.of("traceparent", "00-aa-bb-01"));
        Reservation r = legacy(frame);
        when(ops.reserve()).thenReturn(r);

        builder().build().poll();

        ArgumentCaptor<String> record = ArgumentCaptor.forClass(String.class);
        verify(ops).deadLetter(eq(r), record.capture());
        RedisFrame.Unframed unframed = RedisFrame.unframe(record.getValue());
        assertEquals("00-aa-bb-01", unframed.headers().get("traceparent"));
        assertEquals("unknown_urn", EnvelopeCodec.decode(unframed.body()).deadLetter().reason());
    }

    @Test
    void unknownUrnRecordFallsBackToTheStoredBodyWhenTheRecordCannotBeBuilt() {
        String stored = envelope(1);
        Reservation r = legacy(stored);
        Envelope envelope = EnvelopeCodec.decode(stored);
        // Framing the legacy record walks the headers; a map that throws makes the build fail.
        Map<String, String> explodingHeaders = new AbstractMap<>() {
            @Override
            public Set<Map.Entry<String, String>> entrySet() {
                throw new IllegalStateException("boom");
            }
        };

        String record = builder().build().unknownUrnRecord(
            envelope, explodingHeaders, new UnknownUrnException(URN), r, false);

        assertEquals(stored, record);
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
        assertThrows(IllegalArgumentException.class, () -> builder().failureDelaySeconds(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> builder().failureDelaySeconds(Double.POSITIVE_INFINITY));
    }
}
