package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Direct coverage of the narrow frame-grammar parser {@link JsonLite}: the value shapes it
 * accepts (string / integer / nested string→string object, with the full escape table) and
 * the malformed inputs it rejects by returning {@code null} (so {@link RedisFrame#unframe}
 * falls back to a bare value).
 */
class JsonLiteTest {

    @Test
    void parsesAFullFrameObjectWithAllValueShapes() {
        Map<String, Object> obj = JsonLite.parseFrame(
            "{\"__bq_frame\":1,\"headers\":{\"traceparent\":\"00-aa-bb-01\"},\"body\":\"envelope\"}");
        assertEquals(1L, obj.get("__bq_frame"));
        assertEquals("envelope", obj.get("body"));
        assertEquals(Map.of("traceparent", "00-aa-bb-01"), obj.get("headers"));
    }

    @Test
    void parsesEmptyObjectsAndEmptyHeaders() {
        assertTrue(JsonLite.parseFrame("{}").isEmpty());
        Map<String, Object> obj = JsonLite.parseFrame("{\"__bq_frame\":1,\"headers\":{},\"body\":\"x\"}");
        assertEquals(Map.of(), obj.get("headers"));
    }

    @Test
    void decodesEveryStringEscape() {
        // Every JSON string escape (quote, backslash, slash, n, r, t, b, f) plus a
        // 4-hex unicode escape, all in one string value.
        Map<String, Object> obj = JsonLite.parseFrame(
            "{\"body\":\"q\\\"s\\\\b\\/n\\nr\\rt\\tb\\bf\\fu\\u263a\"}");
        assertEquals("q\"s\\b/n\nr\rt\tb\bf\fu☺", obj.get("body"));
    }

    @Test
    void rejectsMalformedInputByReturningNull() {
        assertNull(JsonLite.parseFrame("{"));                  // unexpected end of input
        assertNull(JsonLite.parseFrame("{\"a\":1"));           // missing closing brace
        assertNull(JsonLite.parseFrame("{\"a\":1 \"b\":2}"));  // missing comma
        assertNull(JsonLite.parseFrame("{\"a\":}"));           // missing value
        assertNull(JsonLite.parseFrame("{\"a\":\"x\\q\"}"));   // bad escape
        assertNull(JsonLite.parseFrame("{\"a\" 1}"));          // missing colon
        assertNull(JsonLite.parseFrame("{}trailing"));         // trailing garbage
        assertNull(JsonLite.parseFrame("[1,2]"));              // not an object at top level
        assertNull(JsonLite.parseFrame("{\"a\":true}"));       // unsupported value type
    }
}
