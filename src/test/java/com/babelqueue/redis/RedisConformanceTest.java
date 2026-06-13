package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import io.lettuce.core.LMoveArgs;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Redis binding conformance against the vendored canonical suite's {@code redis} block:
 * §1 <em>payload identity</em>. Redis lists carry no native metadata, so there is no
 * property projection to assert — the one cross-SDK invariant is that the queue element
 * IS the canonical envelope JSON, byte-for-byte, with no wrapping and no added fields,
 * and that reserve returns it unchanged. No Redis, no network (the command seam is mocked).
 */
class RedisConformanceTest {

    private static final String QUEUE = "orders";

    @SuppressWarnings("unchecked")
    private static RedisCommands<String, String> mockRedis() {
        return Mockito.mock(RedisCommands.class);
    }

    private static String resource(String path) throws Exception {
        try (InputStream in = RedisConformanceTest.class.getResourceAsStream("/conformance/" + path)) {
            if (in == null) {
                throw new IllegalStateException("vendored conformance resource missing: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static JSONObject redisBlock() throws Exception {
        return new JSONObject(resource("manifest.json")).getJSONObject("redis");
    }

    /** The body produced for the fixture is the canonical envelope, unwrapped — no extra fields. */
    @Test
    void produceStoresTheCanonicalEnvelopeVerbatim() throws Exception {
        JSONObject identity = redisBlock().getJSONObject("payload_identity");
        String fixture = resource(identity.getString("envelope_file"));
        Envelope fixtureEnvelope = EnvelopeCodec.decode(fixture);

        RedisCommands<String, String> redis = mockRedis();
        // Re-publish the fixture's own (urn, data), continuing its trace, so the produced
        // envelope is the canonical encoding of that exact logical message.
        RedisPublisher.create(redis, QUEUE)
            .publish(fixtureEnvelope.job(), fixtureEnvelope.data(), fixtureEnvelope.traceId());

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        verify(redis).rpush(eq(QUEUE), body.capture());
        String stored = body.getValue();

        // The stored element decodes to a canonical envelope and re-encodes to itself:
        // proof of NO wrapping (no outer job-structure like BullMQ) and NO added fields.
        Envelope decoded = EnvelopeCodec.decode(stored);
        assertEquals(stored, EnvelopeCodec.encode(decoded),
            "the stored list element must be the canonical envelope, unwrapped and unmutated");

        // The contract-identifying fields survive verbatim (id/trace continue the fixture).
        assertEquals(fixtureEnvelope.job(), decoded.job());
        assertEquals(fixtureEnvelope.traceId(), decoded.traceId());
        assertEquals(fixtureEnvelope.data(), decoded.data());
        assertEquals(fixtureEnvelope.attempts(), decoded.attempts());
    }

    /**
     * The fixture bytes round-trip through produce ({@code RPUSH}) → reserve ({@code BLMOVE})
     * unchanged: the consumer hands the handler the exact stored bytes, no mutation.
     */
    @Test
    void produceThenReserveReturnsIdenticalBytes() throws Exception {
        String fixture = resource(redisBlock().getJSONObject("payload_identity").getString("envelope_file"));
        String canonical = EnvelopeCodec.encode(EnvelopeCodec.decode(fixture)); // the byte form this SDK stores

        RedisCommands<String, String> redis = mockRedis();
        // Reserve replays exactly what produce stored (a real Redis BLMOVE returns the element verbatim).
        when(redis.blmove(eq(QUEUE), eq(QUEUE + ":processing"), any(LMoveArgs.class), anyLong()))
            .thenReturn(canonical, (String) null);

        String[] delivered = {null};
        RedisConsumer.builder(redis, QUEUE)
            .handler(EnvelopeCodec.urn(EnvelopeCodec.decode(canonical)), (env, raw) -> delivered[0] = raw)
            .build()
            .poll();

        assertEquals(canonical, delivered[0], "reserve must return the stored envelope bytes unchanged");
        verify(redis).lrem(QUEUE + ":processing", 1, canonical); // ack removes the exact bytes
    }
}
