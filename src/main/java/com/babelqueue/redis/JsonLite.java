package com.babelqueue.redis;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A minimal, dependency-free reader for the Redis transport's own {@code __bq_frame}
 * header frame (ADR-0028) — and nothing more. The frame is a flat JSON object whose values
 * are exactly: an integer (the {@code __bq_frame} version), a string (the {@code body},
 * the raw wire envelope), and a nested string→string object (the {@code headers}). This
 * parser supports only that shape; it is never used on the wire envelope, which the core
 * codec owns. Any input it cannot parse — including a perfectly valid but differently
 * shaped JSON document — yields {@code null}, so {@link RedisFrame#unframe} falls back to
 * treating the value as a bare envelope.
 *
 * <p>Keeping the grammar this narrow keeps the transport zero-dependency (GR-7) without
 * carrying a general-purpose JSON parser's surface.
 */
final class JsonLite {

    private final String s;
    private int pos;

    private JsonLite(String s) {
        this.s = s;
    }

    /**
     * Parses {@code raw} as a frame object: a JSON object mapping string keys to a string,
     * an integer, or a nested string→string object. Returns the map, or {@code null} on any
     * deviation from that shape.
     */
    static Map<String, Object> parseFrame(String raw) {
        try {
            JsonLite p = new JsonLite(raw);
            Map<String, Object> obj = p.parseObject(true);
            return p.atEnd() ? obj : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Parses an object; {@code topLevel} values may be string/int/object, nested values string only. */
    private Map<String, Object> parseObject(boolean topLevel) {
        expect('{');
        Map<String, Object> obj = new LinkedHashMap<>();
        if (peek() == '}') {
            pos++;
            return obj;
        }
        while (true) {
            String key = parseString();
            expect(':');
            obj.put(key, topLevel ? parseTopValue() : parseString());
            char c = next();
            if (c == '}') {
                return obj;
            }
            if (c != ',') {
                throw new IllegalStateException("expected , or }");
            }
        }
    }

    /** A top-level frame value is a string, a non-negative integer, or a nested string→string object. */
    private Object parseTopValue() {
        char c = peek();
        if (c == '"') {
            return parseString();
        }
        if (c == '{') {
            return parseObject(false);
        }
        return parseInt();
    }

    private String parseString() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (true) {
            char c = next();
            if (c == '"') {
                return sb.toString();
            }
            if (c == '\\') {
                char e = next();
                switch (e) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'u' -> {
                        sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                        pos += 4;
                    }
                    default -> throw new IllegalStateException("bad escape");
                }
            } else {
                sb.append(c);
            }
        }
    }

    private Long parseInt() {
        int start = pos;
        while (pos < s.length() && s.charAt(pos) >= '0' && s.charAt(pos) <= '9') {
            pos++;
        }
        if (pos == start) {
            throw new IllegalStateException("expected integer");
        }
        return Long.parseLong(s.substring(start, pos));
    }

    private boolean atEnd() {
        return pos == s.length();
    }

    private char peek() {
        if (pos >= s.length()) {
            throw new IllegalStateException("unexpected end of input");
        }
        return s.charAt(pos);
    }

    private char next() {
        char c = peek();
        pos++;
        return c;
    }

    private void expect(char c) {
        if (next() != c) {
            throw new IllegalStateException("expected " + c);
        }
    }
}
