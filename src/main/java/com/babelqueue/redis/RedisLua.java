package com.babelqueue.redis;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The Lua scripts the Redis transport runs through {@code EVALSHA} (broker-bindings §1.1, §1.7).
 *
 * <p>Redis executes a script atomically, which is what makes every multi-key transition here (reserve,
 * migrate, sweep, ack, release) safe against a concurrent worker. {@code cjson} is used only by
 * {@link #ZSET_RESERVE} — Laravel's own reserve decodes and re-encodes the job there, and a shared queue
 * has to match it; release and delayed publish go through the SDK codec instead.
 */
final class RedisLua {

    private RedisLua() {}

    /**
     * Zset layout: move due members of {@code KEYS[1]} ({@code :delayed} or {@code :reserved}) to the ready
     * list {@code KEYS[2]}, pushing one {@code :notify} token ({@code KEYS[3]}) per item. {@code ARGV[1]} =
     * {@code floor(now)}, {@code ARGV[2]} = batch size ({@code -1} = all). Returns the number moved.
     * Pushes happen in {@code unpack} chunks of 100 to stay under Lua's argument limit.
     */
    static final String ZSET_MIGRATE = """
        local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
        if next(val) ~= nil then
            redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)
            for i = 1, #val, 100 do
                local last = math.min(i + 99, #val)
                redis.call('rpush', KEYS[2], unpack(val, i, last))
                for j = i, last do
                    redis.call('rpush', KEYS[3], 1)
                end
            end
        end
        return #val
        """;

    /**
     * Zset layout reserve (Laravel {@code LuaScripts::pop()} equivalent, A1). {@code KEYS} =
     * {@code [ready, reserved, notify]}, {@code ARGV[1]} = reservation deadline score. Returns
     * {@code {job, reserved}} or nil when the queue is empty. A head that cannot be decoded, or whose
     * {@code attempts} is not a number, is written to {@code :reserved} raw and returned as {@code {job, job}}
     * (poison) — the script never fails after the {@code LPOP}. The re-encode is {@code pcall}-guarded too:
     * lua-cjson accepts numbers on decode (e.g. {@code 1e400}, {@code NaN}) that it refuses on encode, and Redis
     * Lua has no rollback, so an encode failure falls through to the same poison path.
     */
    static final String ZSET_RESERVE = """
        local job = redis.call('lpop', KEYS[1])
        if not job then
            return false
        end
        local ok, payload = pcall(cjson.decode, job)
        if ok and type(payload) == 'table' and type(payload['attempts']) == 'number' then
            payload['attempts'] = payload['attempts'] + 1
            local eok, reserved = pcall(cjson.encode, payload)
            if eok then
                redis.call('zadd', KEYS[2], ARGV[1], reserved)
                redis.call('lpop', KEYS[3])
                return {job, reserved}
            end
        end
        redis.call('zadd', KEYS[2], ARGV[1], job)
        return {job, job}
        """;

    /**
     * Zset layout release (A2): {@code KEYS} = {@code [reserved, delayed]}, {@code ARGV} = {@code [member,
     * score, body]}. Removes the reservation and schedules the SDK-encoded body in one step.
     */
    static final String ZSET_RELEASE = """
        redis.call('zrem', KEYS[1], ARGV[1])
        redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])
        return 1
        """;

    /**
     * Zset layout dead-letter: {@code KEYS} = {@code [reserved, dlq]}, {@code ARGV} = {@code [member, body]}.
     * Writes the DLQ entry only if this call removed the reservation.
     */
    static final String ZSET_DEAD_LETTER = """
        if redis.call('zrem', KEYS[1], ARGV[1]) == 1 then
            redis.call('rpush', KEYS[2], ARGV[2])
            return 1
        end
        return 0
        """;

    /** Zset layout publish: {@code KEYS} = {@code [ready, notify]}, {@code ARGV[1]} = body. */
    static final String ZSET_PUSH = """
        redis.call('rpush', KEYS[1], ARGV[1])
        redis.call('rpush', KEYS[2], 1)
        return 1
        """;

    /**
     * Legacy layout reserve: {@code KEYS} = {@code [ready, processing, deadlines]}, {@code ARGV} =
     * {@code [deadline, token]}. Atomically moves the head to {@code :processing} and records the deadline
     * member {@code <token>:<raw>}; {@code deadline} may be {@code +inf} (recovery disabled).
     */
    static final String LEGACY_RESERVE = """
        local raw = redis.call('lmove', KEYS[1], KEYS[2], 'LEFT', 'RIGHT')
        if not raw then
            return false
        end
        redis.call('zadd', KEYS[3], ARGV[1], ARGV[2] .. ':' .. raw)
        return raw
        """;

    /**
     * Legacy layout sweep: {@code KEYS} = {@code [ready, processing, deadlines]}, {@code ARGV} =
     * {@code [floor(now), batch]}. Moves each expired reservation's raw body (member bytes from offset 34)
     * back to the ready list unchanged. Orphans (no matching {@code :processing} item) and malformed members
     * are only removed from the deadline zset. Returns the number moved.
     */
    static final String LEGACY_SWEEP = """
        local expired = redis.call('zrangebyscore', KEYS[3], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
        local moved = 0
        for _, member in ipairs(expired) do
            if #member >= 34 and string.sub(member, 33, 33) == ':' then
                local raw = string.sub(member, 34)
                if redis.call('lrem', KEYS[2], 1, raw) == 1 then
                    redis.call('rpush', KEYS[1], raw)
                    moved = moved + 1
                end
            end
            redis.call('zrem', KEYS[3], member)
        end
        return moved
        """;

    /**
     * Legacy layout ack (K1 gate): {@code KEYS} = {@code [processing, deadlines]}, {@code ARGV} =
     * {@code [member, raw]}. Only the holder of the current deadline member may remove the item; returns 0
     * (touching nothing) when the member is gone — the reservation expired and was swept.
     */
    static final String LEGACY_ACK = """
        if redis.call('zrem', KEYS[2], ARGV[1]) == 1 then
            redis.call('lrem', KEYS[1], 1, ARGV[2])
            return 1
        end
        return 0
        """;

    /**
     * Legacy layout move (release / dead-letter), same K1 gate as {@link #LEGACY_ACK}: {@code KEYS} =
     * {@code [processing, deadlines, dest]}, {@code ARGV} = {@code [member, raw, body]}. Pushes {@code body}
     * to {@code dest} in the same script, only when the gate passes.
     */
    static final String LEGACY_MOVE = """
        if redis.call('zrem', KEYS[2], ARGV[1]) == 1 then
            redis.call('lrem', KEYS[1], 1, ARGV[2])
            redis.call('rpush', KEYS[3], ARGV[3])
            return 1
        end
        return 0
        """;

    /** The lowercase-hex SHA-1 of {@code script}, the {@code EVALSHA} digest. */
    static String sha1(String script) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(script.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(40);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is unavailable", e);
        }
    }
}
