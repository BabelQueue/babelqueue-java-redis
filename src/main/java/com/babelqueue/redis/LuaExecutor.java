package com.babelqueue.redis;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

/** Runs Lua through {@code EVALSHA}, loading the script with {@code EVAL} on the first {@code NOSCRIPT}. */
final class LuaExecutor {

    private final RedisCommands<String, String> redis;

    LuaExecutor(RedisCommands<String, String> redis) {
        this.redis = redis;
    }

    <T> T run(String script, ScriptOutputType type, String[] keys, String... args) {
        try {
            return redis.evalsha(RedisLua.sha1(script), type, keys, args);
        } catch (RedisNoScriptException noScript) {
            return redis.eval(script, type, keys, args);
        }
    }

    /**
     * Like {@link #run} but byte-exact in both directions: the arguments ({@code String} = UTF-8, {@code byte[]} =
     * as is) are sent raw and the reply comes back as {@code byte[]} (a {@code VALUE}) or a list of {@code byte[]}
     * / {@code Long} / {@code null} (a {@code MULTI}), never passed through the connection's text codec. Only
     * {@link ScriptOutputType#VALUE}, {@link ScriptOutputType#MULTI} and {@link ScriptOutputType#INTEGER} are used.
     */
    <T> T runBytes(String script, ScriptOutputType type, String[] keys, Object... args) {
        try {
            return dispatch(CommandType.EVALSHA, type, new ScriptArgs(RedisLua.sha1(script), keys, args));
        } catch (RedisNoScriptException noScript) {
            return dispatch(CommandType.EVAL, type, new ScriptArgs(script, keys, args));
        }
    }

    /** A plain command with raw byte arguments (e.g. {@code ZREM}); the reply is an integer. */
    Long runBytesCommand(CommandType command, String key, Object... values) {
        return dispatch(command, ScriptOutputType.INTEGER, new ByteArgs(key, values));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T> T dispatch(CommandType command, ScriptOutputType type, CommandArgs args) {
        CommandOutput output = switch (type) {
            case VALUE -> new ValueOutput<>(ByteArrayCodec.INSTANCE);
            case MULTI -> new NestedMultiOutput<>(ByteArrayCodec.INSTANCE);
            case INTEGER -> new IntegerOutput<>(ByteArrayCodec.INSTANCE);
            default -> throw new IllegalArgumentException("unsupported script output type " + type);
        };
        return (T) ((RedisCommands) redis).dispatch(command, output, args);
    }
}
