package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.EnvelopeCodec;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The transport-owned {@code __bq_frame} JSON frame (ADR-0028): framed when (and only when)
 * usable headers are present, bare back-compat otherwise, and a clean round-trip that never
 * touches the wire envelope (GR-1).
 */
class RedisFrameTest {

    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    private static String envelope() {
        return EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 7), "orders", "trace-1"));
    }

    @Test
    void framesWhenHeadersArePresentAndRoundTripsExactly() {
        String body = envelope();
        String framed = RedisFrame.frame(body, Map.of("traceparent", TRACEPARENT));

        assertTrue(framed.contains("\"__bq_frame\""));
        assertFalse(body.equals(framed));

        RedisFrame.Unframed back = RedisFrame.unframe(framed);
        assertEquals(body, back.body());                         // bare envelope recovered verbatim
        assertEquals(TRACEPARENT, back.headers().get("traceparent"));
    }

    @Test
    void storesBareEnvelopeByteForByteWhenNoUsableHeaders() {
        String body = envelope();
        assertSame(body, RedisFrame.frame(body, Map.of()));
        assertSame(body, RedisFrame.frame(body, null));

        Map<String, String> blanks = new LinkedHashMap<>();
        blanks.put("", "x");      // blank key
        blanks.put("traceparent", ""); // blank value
        assertSame(body, RedisFrame.frame(body, blanks)); // all dropped -> bare
    }

    @Test
    void unframesABareEnvelopeAsItselfWithEmptyHeaders() {
        String body = envelope();
        RedisFrame.Unframed back = RedisFrame.unframe(body);
        assertEquals(body, back.body());
        assertTrue(back.headers().isEmpty());
    }

    @Test
    void aFrozenEnvelopeNeverLooksLikeAFrame() {
        // The frozen envelope has no __bq_frame key, so it is always treated as bare.
        String body = envelope();
        assertFalse(body.contains("__bq_frame"));
        assertEquals(body, RedisFrame.unframe(body).body());
    }

    @Test
    void malformedOrSentinelLessValuesFallBackToBare() {
        // Looks like JSON, mentions the sentinel substring in data, but is not a valid frame.
        String tricky = "{\"data\":\"contains __bq_frame text\"}";
        assertEquals(tricky, RedisFrame.unframe(tricky).body());
        assertTrue(RedisFrame.unframe(tricky).headers().isEmpty());

        assertEquals("not json", RedisFrame.unframe("not json").body());
        assertEquals("", RedisFrame.unframe("").body());
        // A frame whose body escapes quotes/backslashes round-trips.
        String hard = "{\"job\":\"x\",\"q\":\"a\\\"b\\\\c\"}";
        String framed = RedisFrame.frame(hard, Map.of("traceparent", TRACEPARENT));
        assertEquals(hard, RedisFrame.unframe(framed).body());
    }

    @Test
    void headersAreSanitizedAndSortedDeterministically() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("tracestate", "vendor=v");
        headers.put("traceparent", TRACEPARENT);
        String framed = RedisFrame.frame(envelope(), headers);
        // sorted: tracestate would come after traceparent
        assertTrue(framed.indexOf("\"traceparent\"") < framed.indexOf("\"tracestate\""));
        RedisFrame.Unframed back = RedisFrame.unframe(framed);
        assertEquals(2, back.headers().size());
    }

    @Test
    void framedBodyWithUnicodeAndControlCharsRoundTrips() {
        // A body with a newline + non-ASCII survives escape/unescape (body is verbatim).
        String body = "{\"job\":\"x\",\"emoji\":\"smiley\\u263a\",\"nl\":\"a\\nb\"}";
        String framed = RedisFrame.frame(body, Map.of("traceparent", TRACEPARENT));
        assertEquals(body, RedisFrame.unframe(framed).body());
    }

    @Test
    void aZeroVersionFrameIsTreatedAsBare() {
        // A JSON object that mentions the sentinel but with a falsy (0) version is not a frame.
        String notAFrame = "{\"__bq_frame\":0,\"headers\":{\"traceparent\":\"" + TRACEPARENT + "\"},"
            + "\"body\":\"" + "x" + "\"}";
        RedisFrame.Unframed back = RedisFrame.unframe(notAFrame);
        assertEquals(notAFrame, back.body());
        assertTrue(back.headers().isEmpty());
    }

    @Test
    void aFrameWithNoHeadersObjectUnframesWithEmptyHeaders() {
        String body = envelope();
        // Hand-build a frame lacking a "headers" key (still a valid frame: sentinel + body).
        String framed = "{\"__bq_frame\":1,\"body\":" + jsonString(body) + "}";
        RedisFrame.Unframed back = RedisFrame.unframe(framed);
        assertEquals(body, back.body());
        assertTrue(back.headers().isEmpty());
    }

    @Test
    void aFrameWhoseBodyIsNotAStringFallsBackToBare() {
        // sentinel present but "body" is an integer -> not a usable frame.
        String bad = "{\"__bq_frame\":1,\"body\":42}";
        assertEquals(bad, RedisFrame.unframe(bad).body());
    }

    @Test
    void aValueWithTheSentinelSubstringButInvalidGrammarFallsBackToBare() {
        // Contains the "__bq_frame" sentinel substring so it passes the cheap guard, but the
        // JSON grammar is invalid -> parseFrame returns null -> bare.
        String bad = "{\"__bq_frame\":1,\"body\":\"x\" trailing-garbage}";
        assertEquals(bad, RedisFrame.unframe(bad).body());
        assertTrue(RedisFrame.unframe(bad).headers().isEmpty());
    }

    @Test
    void framingEscapesRealControlCharactersInTheBody() {
        // A body containing literal control characters exercises writeString's escape table.
        String body = "line1\nline2\treturn\rback\bff\fend\u0001x";
        String framed = RedisFrame.frame(body, Map.of("traceparent", TRACEPARENT));
        assertTrue(framed.contains("\\u0001"));
        assertEquals(body, RedisFrame.unframe(framed).body());
    }

    private static String jsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.append('"').toString();
    }
}
