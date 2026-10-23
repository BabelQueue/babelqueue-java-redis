package com.babelqueue.redis;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

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
}
