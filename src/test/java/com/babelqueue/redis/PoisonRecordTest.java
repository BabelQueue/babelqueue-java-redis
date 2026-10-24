package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.JsonValues;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** ADR-0035 DLQ record shapes written by the local {@link PoisonRecord} writer (Redis poison path). */
class PoisonRecordTest {

    private static final String BLOCK = "{\"reason\":\"poison\"}";

    private static String build(String body) {
        return PoisonRecord.build(body.getBytes(StandardCharsets.UTF_8), BLOCK);
    }

    @Test
    void splicesTheBlockIntoAStrictJsonObjectKeepingTheOriginalBytes() {
        String body = "{\"a\": 1 , \"b\":[1e2, \"x\"]}";
        assertEquals("{\"a\": 1 , \"b\":[1e2, \"x\"],\"dead_letter\":" + BLOCK + "}", build(body));
        assertEquals("{\"dead_letter\":" + BLOCK + "}", build("{}"));
    }

    @Test
    void wrapsNonObjectsAndObjectsThatCannotBeSpliced() {
        for (String body : new String[] {"not json", "[1,2]", "{\"a\":1} trailing", "{\"dead_letter\":1}", "{\"a\":NaN}"}) {
            Map<?, ?> record = (Map<?, ?>) JsonValues.decode(build(body));
            assertEquals(body, record.get("raw"), body);
            assertFalse(record.containsKey("raw_encoding"), body);
            assertEquals("poison", ((Map<?, ?>) record.get("dead_letter")).get("reason"), body);
        }
    }

    @Test
    void invalidUtf8IsCarriedAsPaddedBase64NeverSubstituted() {
        byte[] bytes = {'{', '"', 'a', '"', ':', '"', (byte) 0xff, (byte) 0xfe, '"', '}'};
        Map<?, ?> record = (Map<?, ?>) JsonValues.decode(PoisonRecord.build(bytes, BLOCK));
        assertEquals("base64", record.get("raw_encoding"));
        assertEquals(Base64.getEncoder().encodeToString(bytes), record.get("raw"));
        assertFalse(((String) record.get("raw")).contains("�"));
        assertTrue(record.containsKey("dead_letter"));
    }

    @Test
    void rejectsADeadLetterThatIsNotASingleJsonObject() {
        for (String bad : new String[] {null, "", "[]", "1", "\"x\"", "null", "{", "{} {}", "{\"a\":1},\"b\":2", "{\"a\":1}x"}) {
            assertThrows(IllegalArgumentException.class, () -> PoisonRecord.build(new byte[] {'{', '}'}, bad), String.valueOf(bad));
        }
    }

    @Test
    void acceptsEveryStrictJsonShapeTheScannerKnows() {
        String[] valid = {
            "  {\t\"a\" :\r\n 1 }  ",
            "{\"a\":\"\\\"\\\\\\/\\b\\f\\n\\r\\t\\u0041\\u00E9\\ud83d\\ude00 plain\"}",
            "{\"a\":[true,false,null,-0,0,0.5,1E+2,-12.5e-3,2e5,[],{},\"\"]}",
            "{\"a\":{\"b\":{\"c\":[1,[2,[3]]]}},\"d\":[{\"e\":1},{\"f\":[]}]}",
            "{\"a\":{\"dead_letter\":1},\"b\":[{\"dead_letter\":2}]}",
            "{\"a\":" + "[".repeat(100) + "]".repeat(100) + "}",
        };
        for (String body : valid) {
            assertTrue(PoisonRecord.isSpliceableObject(body), body);
        }
    }

    @Test
    void rejectsEveryNonStrictOrUnspliceableObject() {
        String[] invalid = {
            "", "   ", "{", "{\"a\"", "{\"a\":", "{\"a\":1", "{\"a\":1,", "{\"a\":1,}", "{a:1}", "{\"a\" 1}",
            "{\"a\":1 \"b\":2}", "{\"a\":[1,]}", "{\"a\":[1}", "{\"a\":{\"b\":1]}", "{\"a\":[1,2}",
            "{\"a\":tru}", "{\"a\":nul}", "{\"a\":fals}", "{\"a\":NaN}", "{\"a\":Infinity}",
            "{\"a\":-}", "{\"a\":-", "{\"a\":01}", "{\"a\":1.}", "{\"a\":1e}", "{\"a\":1e+}", "{\"a\":+1}", "{\"a\":.5}",
            "{\"a\":\"x}", "{\"a\":\"\\x\"}", "{\"a\":\"\\", "{\"a\":\"\\u12\"}", "{\"a\":\"\\u12G4\"}",
            "{\"a\":\"\\u\uff10\uff10\uff14\uff11\"}",
            "{\"a\":\"\\ud800\"}", "{\"a\":\"\\ud800x\"}", "{\"a\":\"\\ud800\\u0041\"}", "{\"a\":\"\\udc00\"}",
            "{\"a\":\"line\nbreak\"}", "{\"a\":1}x", "{\"a\":1}{}", "[]", "1",
            "{\"dead_letter\":1}", "{\"x\":1,\"dead_letter\":{}}", "{\"dead\\u005fletter\":1}",
        };
        for (String body : invalid) {
            assertFalse(PoisonRecord.isSpliceableObject(body), body);
        }
    }
}
