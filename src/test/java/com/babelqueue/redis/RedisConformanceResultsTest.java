package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code BQ_CONFORMANCE_RESULTS} writer, exercised against a private recorded-map and a temp file so a
 * unit run never depends on Redis (the cases a Redis-less run cannot execute are recorded as {@code skip}).
 */
class RedisConformanceResultsTest {

    private static JSONObject manifest() throws Exception {
        try (InputStream in = RedisConformanceResultsTest.class.getResourceAsStream("/conformance/manifest.json")) {
            return new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void writesAResultsFileWithSkipRecordsAndUnexecutedCasesAsFail(@TempDir Path dir) throws Exception {
        Map<String, List<String>> expected = RedisConformanceResults.expectedCases(manifest());
        assertFalse(expected.isEmpty(), "the manifest must scope at least one section to the redis binding");

        String section = expected.keySet().iterator().next();
        String skipped = expected.get(section).get(0);
        Map<String, Map<String, String[]>> recorded = new ConcurrentHashMap<>();
        recorded.computeIfAbsent(section, k -> new ConcurrentHashMap<>())
            .put(skipped, new String[] {"skip", "ADR-0033: no Redis reachable"});

        Path target = dir.resolve("results.json");
        RedisConformanceResults.writeTo(target, recorded);

        assertTrue(Files.exists(target));
        JSONObject doc = new JSONObject(Files.readString(target, StandardCharsets.UTF_8));
        assertEquals("babelqueue-java-redis", doc.getString("sdk"));
        assertEquals(RedisConformanceResults.packageVersion(), doc.getString("version"));
        assertEquals("redis", doc.getJSONArray("bindings").getString(0));
        assertFalse(doc.getString("conformance_version").isBlank());

        JSONObject cases = doc.getJSONObject("sections").getJSONObject(section);
        assertEquals(expected.get(section).size(), cases.length());
        JSONObject skip = cases.getJSONObject(skipped);
        assertEquals("skip", skip.getString("status"));
        assertEquals("ADR-0033: no Redis reachable", skip.getString("reason"));

        // A case the runner never recorded is a failure, not a silent omission.
        if (expected.get(section).size() > 1) {
            assertEquals("fail", cases.getJSONObject(expected.get(section).get(1)).getString("status"));
        }
    }

    @Test
    void passRecordsCarryNoReason(@TempDir Path dir) throws Exception {
        Map<String, List<String>> expected = RedisConformanceResults.expectedCases(manifest());
        String section = expected.keySet().iterator().next();
        String id = expected.get(section).get(0);
        Map<String, Map<String, String[]>> recorded = new ConcurrentHashMap<>();
        recorded.computeIfAbsent(section, k -> new ConcurrentHashMap<>()).put(id, new String[] {"pass", null});

        Path target = dir.resolve("results.json");
        RedisConformanceResults.writeTo(target, recorded);

        JSONObject result = new JSONObject(Files.readString(target, StandardCharsets.UTF_8))
            .getJSONObject("sections").getJSONObject(section).getJSONObject(id);
        assertEquals("pass", result.getString("status"));
        assertFalse(result.has("reason"));
    }
}
