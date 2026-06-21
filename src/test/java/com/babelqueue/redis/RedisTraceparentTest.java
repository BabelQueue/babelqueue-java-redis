package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import io.lettuce.core.LMoveArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * The §1 Redis wiring of out-of-band {@code traceparent} (ADR-0028) over the transport-owned
 * {@code __bq_frame}: produce frames a header-carrying publish (bare otherwise), consume
 * unframes and surfaces the headers to a {@link RedisConsumer.HeaderHandler}, and a
 * publish→consume round-trip makes the consumer span a true child of the producer span. The
 * {@code LREM} ack handle stays the stored frame. No Redis, no network (the
 * {@code RedisCommands} seam is mocked).
 */
class RedisTraceparentTest {

    private static final String QUEUE = "orders";
    private static final String PROCESSING = "orders:processing";
    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    @SuppressWarnings("unchecked")
    private static RedisCommands<String, String> mockRedis() {
        return Mockito.mock(RedisCommands.class);
    }

    @Test
    void publishWithHeadersRpushesAFrameWrappingTheBareEnvelope() {
        RedisCommands<String, String> redis = mockRedis();
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), QUEUE, "trace-1");

        RedisPublisher.create(redis, QUEUE).publishWithHeaders(env, Map.of("traceparent", TRACEPARENT));

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(redis).rpush(eq(QUEUE), value.capture());
        String stored = value.getValue();
        assertTrue(stored.contains("\"__bq_frame\""));
        // GR-1: traceparent rides the frame, not the wire envelope; GR-4: trace_id preserved.
        RedisFrame.Unframed back = RedisFrame.unframe(stored);
        assertFalse(back.body().contains("traceparent"));
        assertEquals(TRACEPARENT, back.headers().get("traceparent"));
        assertEquals("trace-1", EnvelopeCodec.decode(back.body()).traceId());
    }

    @Test
    void headerlessPublishWithHeadersStoresTheBareEnvelope() {
        RedisCommands<String, String> redis = mockRedis();
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), QUEUE, "trace-1");

        RedisPublisher.create(redis, QUEUE).publishWithHeaders(env, Map.of());

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(redis).rpush(eq(QUEUE), value.capture());
        assertEquals(EnvelopeCodec.encode(env), value.getValue()); // byte-identical, no frame
    }

    @Test
    void consumeUnframesAndSurfacesHeadersAndAcksTheStoredFrame() {
        RedisCommands<String, String> redis = mockRedis();
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), QUEUE, "trace-1");
        String frame = RedisFrame.frame(EnvelopeCodec.encode(env), Map.of("traceparent", TRACEPARENT));
        when(redis.blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong()))
            .thenReturn(frame, (String) null);

        Map<String, String>[] seenHeaders = new Map[]{null};
        String[] seenBody = {null};
        RedisConsumer.builder(redis, QUEUE)
            .handler("urn:babel:orders:created", (e, body, headers) -> {
                seenBody[0] = body;
                seenHeaders[0] = headers;
            })
            .build()
            .poll();

        assertEquals(EnvelopeCodec.encode(env), seenBody[0]);          // bare envelope to the handler
        assertEquals(TRACEPARENT, seenHeaders[0].get("traceparent"));  // headers surfaced
        verify(redis).lrem(PROCESSING, 1, frame);                      // ack handle is the stored frame
    }

    @Test
    void bareValueConsumesWithEmptyHeadersBackCompat() {
        RedisCommands<String, String> redis = mockRedis();
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("x", 1), QUEUE, "trace-1");
        String bare = EnvelopeCodec.encode(env);
        when(redis.blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong()))
            .thenReturn(bare, (String) null);

        Map<String, String>[] seenHeaders = new Map[]{null};
        RedisConsumer.builder(redis, QUEUE)
            .handler("urn:babel:orders:created", (e, body, headers) -> seenHeaders[0] = headers)
            .build()
            .poll();

        assertTrue(seenHeaders[0].isEmpty());
        verify(redis).lrem(PROCESSING, 1, bare); // bare value is its own ack handle
    }

    @Test
    void endToEndTraceparentMakesConsumerSpanAChildOfTheProducerSpan() throws Exception {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider provider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
        Tracer tracer = sdk.getTracer("test");

        RedisCommands<String, String> redis = mockRedis();
        RedisPublisher publisher = RedisPublisher.create(redis, QUEUE);

        // Producer: a PRODUCER span injects its traceparent onto the frame headers via the
        // otel HeaderSender seam -> RedisPublisher.publishWithHeaders.
        com.babelqueue.otel.Tracing.publish(
            tracer, "urn:babel:orders:created", Map.of("order_id", 7), QUEUE,
            (envelope, headers) -> publisher.publishWithHeaders(envelope, headers));

        // Feed the produced frame back through BLMOVE.
        ArgumentCaptor<String> stored = ArgumentCaptor.forClass(String.class);
        verify(redis).rpush(eq(QUEUE), stored.capture());
        when(redis.blmove(eq(QUEUE), eq(PROCESSING), any(LMoveArgs.class), anyLong()))
            .thenReturn(stored.getValue(), (String) null);

        // Consumer: wrapHandler reads the surfaced frame headers and parents the CONSUMER span
        // on the carried traceparent.
        RedisConsumer.builder(redis, QUEUE)
            .handler("urn:babel:orders:created", (env, body, headers) ->
                com.babelqueue.otel.Tracing.wrapHandler(tracer, e -> { }, () -> headers).handle(env))
            .build()
            .poll();

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData producer = spanByName(spans, "publish urn:babel:orders:created");
        SpanData consumer = spanByName(spans, "process urn:babel:orders:created");
        assertEquals(producer.getSpanContext().getSpanId(), consumer.getParentSpanContext().getSpanId());
        assertEquals(producer.getSpanContext().getTraceId(), consumer.getTraceId());
        assertTrue(consumer.getParentSpanContext().isRemote());
        provider.close();
    }

    private static SpanData spanByName(List<SpanData> spans, String name) {
        return spans.stream().filter(s -> s.getName().equals(name)).findFirst()
            .orElseThrow(() -> new AssertionError("span not found: " + name));
    }
}
