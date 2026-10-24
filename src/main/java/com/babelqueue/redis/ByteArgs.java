package com.babelqueue.redis;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.protocol.CommandArgs;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Plain-command arguments (one key, then values) encoded as raw bytes ({@link ByteArrayCodec}); the counterpart of
 * {@link ScriptArgs} for commands such as {@code ZREM} whose member may not be valid UTF-8.
 */
final class ByteArgs extends CommandArgs<byte[], byte[]> {

    private final List<byte[]> keys = new ArrayList<>();
    private final List<byte[]> values = new ArrayList<>();

    ByteArgs(String key, Object... values) {
        super(ByteArrayCodec.INSTANCE);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        this.keys.add(keyBytes);
        addKey(keyBytes);
        for (Object value : values) {
            byte[] bytes = value instanceof byte[] raw ? raw : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
            this.values.add(bytes);
            addValue(bytes);
        }
    }

    List<byte[]> keyBytes() {
        return keys;
    }

    List<byte[]> valueBytes() {
        return values;
    }
}
