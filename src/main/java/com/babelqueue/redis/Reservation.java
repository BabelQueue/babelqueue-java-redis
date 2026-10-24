package com.babelqueue.redis;

import java.nio.charset.StandardCharsets;

/**
 * One reserved message: what the handler sees and what is needed to ack / release it.
 *
 * @param body        the raw value as reserved: for the zset layout the job exactly as Laravel stored it
 *                    (what the handler sees), for the legacy layout the stored list value (a bare envelope
 *                    or a transport frame)
 * @param handle      zset: the member written to {@code :reserved} (cjson re-encoded, or the raw job for a
 *                    poison pop); legacy: the 32-hex reservation token
 * @param poison      zset only - the Lua script could not decode the head (or its {@code attempts} is not a
 *                    number), so it wrote the raw job to {@code :reserved} without incrementing
 * @param bodyBytes   the exact stored bytes of {@code body}, set only when they are not valid UTF-8 ({@code body}
 *                    is then a lossy text view); {@code null} otherwise, where {@code body} is the exact value
 * @param handleBytes zset only - the exact bytes of {@code handle}, set only when they are not valid UTF-8
 */
record Reservation(String body, String handle, boolean poison, byte[] bodyBytes, byte[] handleBytes) {

    Reservation(String body, String handle, boolean poison) {
        this(body, handle, poison, null, null);
    }

    /** The legacy deadline-zset member of this reservation: {@code <token>:<raw>}. */
    String legacyMember() {
        return handle + ":" + body;
    }

    /** The stored value as exact bytes: {@link #bodyBytes} when it is not valid UTF-8, else {@code body} as UTF-8. */
    byte[] exactBody() {
        return bodyBytes != null ? bodyBytes : body.getBytes(StandardCharsets.UTF_8);
    }

    /** The legacy deadline-zset member as exact bytes: {@code <token>:<raw bytes>}. */
    byte[] exactLegacyMember() {
        byte[] token = (handle + ":").getBytes(StandardCharsets.UTF_8);
        byte[] raw = exactBody();
        byte[] member = new byte[token.length + raw.length];
        System.arraycopy(token, 0, member, 0, token.length);
        System.arraycopy(raw, 0, member, token.length, raw.length);
        return member;
    }

    /** The zset {@code :reserved} member as exact bytes. */
    byte[] exactHandle() {
        return handleBytes != null ? handleBytes : handle.getBytes(StandardCharsets.UTF_8);
    }

    /** Whether the stored value is not valid UTF-8 (so it can never be a conformant envelope). */
    boolean hasInvalidUtf8() {
        return bodyBytes != null;
    }

    /** {@code bytes} as text when they round-trip through UTF-8 unchanged, else {@code null} (they are not valid UTF-8). */
    static String exactText(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8);
        return java.util.Arrays.equals(text.getBytes(StandardCharsets.UTF_8), bytes) ? text : null;
    }
}
