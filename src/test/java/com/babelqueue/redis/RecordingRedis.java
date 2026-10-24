package com.babelqueue.redis;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;

/**
 * A mocked {@link RedisCommands} that records every call and answers scripted replies per Lua script, so the
 * queue-state machine can be exercised without a server. Scripts are identified by their SHA-1 (the
 * {@code EVALSHA} digest); a script with no scripted reply returns {@code null}.
 */
final class RecordingRedis {

    /** One recorded call: the Lettuce method name and its (varargs-expanded) arguments. */
    record Call(String method, List<Object> args) {
        /** For {@code evalsha}/{@code eval}: the keys array. */
        String[] keys() {
            return (String[]) args.get(2);
        }

        /** For {@code evalsha}/{@code eval}: the ARGV values. */
        List<String> values() {
            List<String> values = new ArrayList<>();
            for (int i = 3; i < args.size(); i++) {
                values.add((String) args.get(i));
            }
            return values;
        }

        String script() {
            return (String) args.get(0);
        }
    }

    final List<Call> calls = new ArrayList<>();
    final RedisCommands<String, String> redis;
    private final Map<String, Deque<Object>> replies = new HashMap<>();
    private boolean noScriptOnce;

    @SuppressWarnings("unchecked")
    RecordingRedis() {
        this.redis = Mockito.mock(RedisCommands.class, invocation -> {
            String method = invocation.getMethod().getName();
            Object[] args = invocation.getArguments();
            calls.add(new Call(method, Arrays.asList(args)));
            if (method.equals("evalsha")) {
                if (noScriptOnce) {
                    noScriptOnce = false;
                    throw new RedisNoScriptException("NOSCRIPT No matching script");
                }
                return nextReply((String) args[0]);
            }
            if (method.equals("eval")) {
                return nextReply(RedisLua.sha1((String) args[0]));
            }
            if (method.equals("zrem")) {
                return nextReply("zrem");
            }
            if (method.equals("dispatch")) {
                return dispatch(args);
            }
            return Mockito.RETURNS_DEFAULTS.answer(invocation);
        });
    }

    /**
     * The byte-exact path ({@link LuaExecutor#runBytes}, {@code ZREM} with raw members): recorded like the text path
     * ({@code evalsha} / {@code eval} / {@code zrem}, byte arguments decoded as UTF-8 so the usual assertions work)
     * and also kept in {@link #byteCalls} with the exact bytes. Scripted {@code String} replies come back as
     * {@code byte[]}, as a {@code ByteArrayCodec} connection would deliver them; a scripted {@code byte[]} is
     * passed through.
     */
    private Object dispatch(Object[] args) {
        ProtocolKeyword command = (ProtocolKeyword) args[0];
        CommandOutput<?, ?, ?> output = (CommandOutput<?, ?, ?>) args[1];
        if (command == CommandType.ZREM) {
            ByteArgs zrem = (ByteArgs) args[2];
            List<Object> text = new ArrayList<>();
            zrem.keyBytes().forEach(k -> text.add(new String(k, StandardCharsets.UTF_8)));
            zrem.valueBytes().forEach(v -> text.add(new String(v, StandardCharsets.UTF_8)));
            calls.add(new Call("zrem", text));
            byteCalls.add(new ByteCall("zrem", zrem.keyBytes(), zrem.valueBytes()));
            return nextReply("zrem");
        }
        ScriptArgs script = (ScriptArgs) args[2];
        ScriptOutputType type = output instanceof NestedMultiOutput ? ScriptOutputType.MULTI
            : output instanceof ValueOutput ? ScriptOutputType.VALUE : ScriptOutputType.INTEGER;
        String[] keys = script.keyBytes().stream().map(k -> new String(k, StandardCharsets.UTF_8)).toArray(String[]::new);
        List<Object> recorded = new ArrayList<>();
        boolean eval = command == CommandType.EVAL;
        recorded.add(script.scriptOrDigest());
        recorded.add(type);
        recorded.add(keys);
        for (byte[] value : script.valueBytes()) {
            recorded.add(new String(value, StandardCharsets.UTF_8));
        }
        byteCalls.add(new ByteCall(eval ? "eval" : "evalsha", script.keyBytes(), script.valueBytes()));
        if (!eval) {
            if (noScriptOnce) {
                noScriptOnce = false;
                throw new RedisNoScriptException("NOSCRIPT No matching script");
            }
            calls.add(new Call("evalsha", recorded));
            return toBytes(nextReply(script.scriptOrDigest()));
        }
        calls.add(new Call("eval", recorded));
        return toBytes(nextReply(RedisLua.sha1(script.scriptOrDigest())));
    }

    private static Object toBytes(Object reply) {
        if (reply instanceof String text) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        if (reply instanceof List<?> list) {
            List<Object> out = new ArrayList<>();
            for (Object element : list) {
                out.add(toBytes(element));
            }
            return out;
        }
        return reply;
    }

    /** A call that went through the byte-exact path: the exact keys and ARGV bytes. */
    record ByteCall(String method, List<byte[]> keys, List<byte[]> values) {}

    final List<ByteCall> byteCalls = new ArrayList<>();

    /** Script {@code script} will answer {@code reply} (queued; the last reply sticks). */
    RecordingRedis script(String script, Object... reply) {
        Deque<Object> queue = replies.computeIfAbsent(RedisLua.sha1(script), k -> new ArrayDeque<>());
        for (Object r : reply) {
            queue.add(r == null ? NULL : r);
        }
        return this;
    }

    /** {@code zrem} (the zset-layout ack) answers {@code reply}. */
    RecordingRedis zrem(Long reply) {
        replies.computeIfAbsent("zrem", k -> new ArrayDeque<>()).add(reply == null ? NULL : reply);
        return this;
    }

    /** The first {@code evalsha} reply will be a NOSCRIPT error (the executor must fall back to EVAL). */
    RecordingRedis noScriptOnce() {
        this.noScriptOnce = true;
        return this;
    }

    private static final Object NULL = new Object();

    private Object nextReply(String key) {
        Deque<Object> queue = replies.get(key);
        if (queue == null || queue.isEmpty()) {
            return null;
        }
        Object reply = queue.size() > 1 ? queue.poll() : queue.peek();
        return reply == NULL ? null : reply;
    }

    /** The calls of one Lua script (by evalsha/eval), in order. */
    List<Call> scriptCalls(String script) {
        String sha = RedisLua.sha1(script);
        List<Call> out = new ArrayList<>();
        for (Call call : calls) {
            if (call.method().equals("evalsha") && call.args().get(0).equals(sha)) {
                out.add(call);
            } else if (call.method().equals("eval") && RedisLua.sha1((String) call.args().get(0)).equals(sha)) {
                out.add(call);
            }
        }
        return out;
    }

    /** The calls of a plain (non-script) method. */
    List<Call> methodCalls(String method) {
        return calls.stream().filter(c -> c.method().equals(method)).toList();
    }

    /** The script output type of an evalsha call. */
    static ScriptOutputType outputType(Call call) {
        return (ScriptOutputType) call.args().get(1);
    }
}
