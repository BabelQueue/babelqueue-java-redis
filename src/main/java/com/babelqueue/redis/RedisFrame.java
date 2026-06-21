package com.babelqueue.redis;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * The Redis transport's own out-of-band header frame (ADR-0028). Redis lists carry no
 * native per-message metadata channel (unlike AMQP message headers or SQS
 * {@code MessageAttributes}), and the reliable-queue pattern stores the bare body
 * ({@code RPUSH}/{@code BLMOVE}; the {@code LREM} ack handle <b>is</b> the stored value),
 * so to carry a header (e.g. a W3C {@code traceparent}) beside the frozen envelope the
 * transport wraps it in a small JSON frame distinct from the wire envelope:
 *
 * <pre>{@code
 * {"__bq_frame":1,"headers":{"traceparent":"00-..."},"body":"<raw wire envelope>"}
 * }</pre>
 *
 * <p>This is the Java mirror of Go's {@code redis.headerFrame} / PHP's
 * {@code RedisTransport::frameValue}/{@code unframe}. It is NOT the wire envelope (GR-1):
 * {@code body} is the raw, unchanged envelope JSON and {@code headers} travels beside it.
 *
 * <p><b>Framing is opt-in and backward compatible.</b> Only {@link #frame} with a
 * non-empty, non-blank header map writes a frame; a plain publish (or a publish whose
 * headers are all blank) stores the bare envelope byte-for-byte. {@link #unframe} detects
 * a frame by the reserved {@code "__bq_frame"} sentinel — a frozen wire envelope can never
 * carry it — so a bare value consumes with empty headers and {@code body == value}, exactly
 * as before; cross-version queues interoperate. The codec is self-contained so the
 * transport adds no JSON dependency (GR-7).
 */
final class RedisFrame {

    /** The reserved discriminator key + value; its presence tells {@link #unframe} a value is framed. */
    static final String SENTINEL_KEY = "__bq_frame";
    static final int FRAME_VERSION = 1;

    /** The decoded result of {@link #unframe}: the bare envelope body and its out-of-band headers. */
    record Unframed(String body, Map<String, String> headers) {
    }

    private RedisFrame() {}

    /**
     * Returns the Redis list value for {@code body} carrying {@code headers}. With no
     * usable headers (null/empty/all-blank) it returns {@code body} byte-for-byte (bare,
     * back-compatible); otherwise a JSON frame wrapping {@code body} with the sanitized
     * headers. The returned string is exactly what is {@code RPUSH}ed and what the
     * {@code LREM} ack handle must match.
     */
    static String frame(String body, Map<String, String> headers) {
        Map<String, String> clean = sanitize(headers);
        if (clean.isEmpty()) {
            return body;
        }
        StringBuilder sb = new StringBuilder(body.length() + 64);
        sb.append("{\"").append(SENTINEL_KEY).append("\":").append(FRAME_VERSION);
        sb.append(",\"headers\":{");
        boolean first = true;
        for (Map.Entry<String, String> e : clean.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            writeString(sb, e.getKey());
            sb.append(':');
            writeString(sb, e.getValue());
        }
        sb.append("},\"body\":");
        writeString(sb, body);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Splits a reserved Redis list {@code value} into its bare envelope body and
     * out-of-band headers. A value lacking the {@code "__bq_frame"} sentinel (every
     * frozen envelope, and any pre-0028 producer's output) is returned verbatim as the
     * body with empty headers — bare-value back-compat. A malformed or sentinel-less
     * object is likewise treated as bare, so a real envelope is never misread.
     */
    static Unframed unframe(String value) {
        if (value == null || value.isEmpty() || value.charAt(0) != '{'
            || !value.contains("\"" + SENTINEL_KEY + "\"")) {
            return new Unframed(value, Map.of());
        }
        Map<String, Object> obj = JsonLite.parseFrame(value);
        if (obj == null) {
            return new Unframed(value, Map.of());
        }
        Object sentinel = obj.get(SENTINEL_KEY);
        Object body = obj.get("body");
        // The sentinel must be present and truthy, and body must be a string — else bare.
        if (!isTruthy(sentinel) || !(body instanceof String bodyStr)) {
            return new Unframed(value, Map.of());
        }
        Map<String, String> headers = Map.of();
        Object rawHeaders = obj.get("headers");
        if (rawHeaders instanceof Map<?, ?> hmap) {
            Map<String, String> flat = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : hmap.entrySet()) {
                if (e.getKey() != null && e.getValue() instanceof String hv) {
                    flat.put(String.valueOf(e.getKey()), hv);
                }
            }
            headers = sanitize(flat);
        }
        return new Unframed(bodyStr, headers);
    }

    /** Copies {@code headers}, dropping blank keys/values; sorted for deterministic frames. */
    static Map<String, String> sanitize(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        Map<String, String> out = new TreeMap<>();
        for (Map.Entry<String, String> e : headers.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            if (key != null && !key.isEmpty() && value != null && !value.isEmpty()) {
                out.put(key, value);
            }
        }
        return out;
    }

    /** The sentinel is a frame-version integer; a present, non-zero version marks a real frame. */
    private static boolean isTruthy(Object v) {
        return v instanceof Number n && n.longValue() != 0;
    }

    /** Writes a JSON string literal with the minimal required escaping. */
    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
}
