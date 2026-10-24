package com.babelqueue.redis;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.protocol.CommandArgs;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code EVAL} / {@code EVALSHA} arguments encoded as raw bytes ({@link ByteArrayCodec}), so a stored value that is
 * not valid UTF-8 travels to Redis byte for byte instead of being substituted (ADR-0035 section 1). It keeps the
 * keys and values it was built from so a test double can read them back without parsing the wire form.
 */
final class ScriptArgs extends CommandArgs<byte[], byte[]> {

    private final String scriptOrDigest;
    private final List<byte[]> keys = new ArrayList<>();
    private final List<byte[]> values = new ArrayList<>();

    /**
     * @param scriptOrDigest the script body ({@code EVAL}) or its SHA-1 ({@code EVALSHA})
     * @param keys           the {@code KEYS}
     * @param args           the {@code ARGV}: each a {@code String} (sent as UTF-8) or a {@code byte[]} (sent as is)
     */
    ScriptArgs(String scriptOrDigest, String[] keys, Object... args) {
        super(ByteArrayCodec.INSTANCE);
        this.scriptOrDigest = scriptOrDigest;
        add(scriptOrDigest);
        add(keys.length);
        for (String key : keys) {
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            this.keys.add(bytes);
            addKey(bytes);
        }
        for (Object arg : args) {
            byte[] bytes = arg instanceof byte[] raw ? raw : String.valueOf(arg).getBytes(StandardCharsets.UTF_8);
            this.values.add(bytes);
            addValue(bytes);
        }
    }

    String scriptOrDigest() {
        return scriptOrDigest;
    }

    List<byte[]> keyBytes() {
        return keys;
    }

    List<byte[]> valueBytes() {
        return values;
    }
}
