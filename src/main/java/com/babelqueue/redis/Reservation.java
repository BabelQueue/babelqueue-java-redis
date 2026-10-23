package com.babelqueue.redis;

/**
 * One reserved message: what the handler sees and what is needed to ack / release it.
 *
 * @param body    the raw value as reserved: for the zset layout the job exactly as Laravel stored it
 *                (what the handler sees), for the legacy layout the stored list value (a bare envelope
 *                or a transport frame)
 * @param handle  zset: the member written to {@code :reserved} (cjson re-encoded, or the raw job for a
 *                poison pop); legacy: the 32-hex reservation token
 * @param poison  zset only — the Lua script could not decode the head (or its {@code attempts} is not a
 *                number), so it wrote the raw job to {@code :reserved} without incrementing
 */
record Reservation(String body, String handle, boolean poison) {

    /** The legacy deadline-zset member of this reservation: {@code <token>:<raw>}. */
    String legacyMember() {
        return handle + ":" + body;
    }
}
