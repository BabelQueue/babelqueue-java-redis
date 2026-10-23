package com.babelqueue.redis;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
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
            return Mockito.RETURNS_DEFAULTS.answer(invocation);
        });
    }

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
