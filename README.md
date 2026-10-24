# BabelQueue — Redis (Java)

`com.babelqueue:babelqueue-redis` — a Redis transport for
[BabelQueue](https://babelqueue.com), built on the [Lettuce](https://lettuce.io)
client and the framework-agnostic
[`babelqueue-core`](https://github.com/BabelQueue/babelqueue-java).

A canonical-envelope **publisher** and a URN-routed **consumer**, so a Redis-based Java
service speaks the same wire contract (envelope shape, URN identity, trace propagation)
as the PHP/Laravel, Python, Go, Node and .NET SDKs. Implements
[§1 of the broker-bindings contract](https://babelqueue.com) — the reliable-queue list
pattern.

## Install (Maven)

```xml
<dependency>
  <groupId>com.babelqueue</groupId>
  <artifactId>babelqueue-redis</artifactId>
  <version>1.1.0</version>
</dependency>
```

It pulls `babelqueue-core` and `io.lettuce:lettuce-core` transitively.

## Use

```java
RedisClient client = RedisClient.create("redis://localhost:6379");
RedisCommands<String, String> redis = client.connect().sync(); // the command seam

// produce
String id = RedisPublisher.create(redis, "orders")
    .publish("urn:babel:orders:created", Map.of("order_id", 1042));

// consume
RedisConsumer consumer = RedisConsumer.builder(redis, "orders")
    .handler("urn:babel:orders:created", (env, body) -> {
        // env.data(), env.traceId(), env.attempts() ...
    })
    .onError((err, env, body) -> log.warn("bad message", err))
    .build();
consumer.run(); // blocking-reserves until the thread is interrupted
```

Point the `RedisClient` at any Redis (local, cluster via the appropriate client, or a
managed instance). The command seam is Lettuce's `RedisCommands<String, String>`
interface, so it is trivially mockable in your own tests.

## Contract mapping (§1)

| Envelope | Redis |
| :--- | :--- |
| body | the list element — the canonical envelope JSON, **byte-for-byte, no wrapping** |
| `job` (URN) | read from the body and routed consumer-side (Redis lists carry no native metadata) |
| produce | `RPUSH <queue> <envelope>` |
| reserve | `BLMOVE <queue> <queue>:processing LEFT RIGHT` (head → tail; crash-safe in-flight) |
| ack | `LREM <queue>:processing 1 <envelope>` |
| `attempts` | taken from the body unchanged (Redis has no native delivery counter) |

Redis lists have **no native attribute channel**, so — unlike the SQS/RabbitMQ/Kafka
bindings — there is **no property projection**. The single cross-SDK invariant is
**payload identity**: the stored element is the exact envelope bytes, with no outer
job-structure and no added fields.

Retry is **at-least-once**: in the default layout a throwing handler leaves the message on the
`<queue>:processing` list (a recovery sweep can requeue it); a successful handler `LREM`s
it (the zset layout releases it with a backoff when one is configured, see below). The poll loop never stops on a bad message — observe via `onError` / `onUnknownUrn`.

A message whose URN has **no handler** is not retried: with no `onUnknownUrn` hook it is reported to `onError`
and **dead-lettered** to `<queue>.dlq` with `dead_letter.reason = "unknown_urn"` (the reservation is removed
atomically by the same script that writes the DLQ record, in both layouts), so it cannot loop after
`retry_after`. If you register an `onUnknownUrn` hook, it takes over: the hook runs and the message is acked
(no DLQ record).
The envelope is unchanged (`schema_version` stays `1`); Redis is purely additive.

> **Scope.** The default (`RedisLayout.LEGACY`) is a **Java-owned reliable queue**,
> mirroring the Go runtime's reliable-queue mechanism. For a queue **shared with the Laravel
> driver** use the zset layout below.

## Laravel-compatible layout (`RedisLayout.ZSET`, ADR-0033)

```java
RedisPublisher publisher = RedisPublisher.builder(redis, "orders")
    .layout(RedisLayout.ZSET).build();                      // keys: queues:orders[:delayed|:reserved|:notify]
publisher.publishDelayed("urn:babel:orders:created", Map.of("order_id", 1), 30);

RedisConsumer consumer = RedisConsumer.builder(redis, "orders")
    .layout(RedisLayout.ZSET)
    .retryAfterSeconds(90)            // reservation lease; .retryAfterDisabled() turns recovery off
    .migrationBatchSize(1000)         // optional cap per migrate pass (default: unbounded)
    .handler("urn:babel:orders:created", (env, body) -> { /* ... */ })
    .build();
```

- Reserve, release, migrate and dead-letter are each **one Lua script** (`EVALSHA`, with an
  `EVAL` fallback on `NOSCRIPT`), so a crash can never lose or duplicate a message in transit.
  Due `:delayed` members and expired `:reserved` members are migrated back to the ready list
  before every reserve. A non-JSON member is dead-lettered instead of blocking the queue.
- A delayed publish scores the member `floor(now) + ceil(delay)` (seconds).
- A throwing handler is **released** to `:delayed` with `attempts + 1` (design A2) as soon as a delay
  source is configured. The delay is, in order of precedence:
  1. `failureDelaySeconds(n)` &mdash; a constant `n` seconds (an explicit value always wins);
  2. the retry backoff (ADR-0034 §9), `d(k) = min(max, base * multiplier^(k-1))` with `k` the 1-based
     delivery number (the `attempts` the handler saw): `retryBackoff(base)`,
     `retryBackoffMultiplier(m >= 1)`, `retryBackoffMax(max)` (`0` = unlimited) and
     `retryJitter("none" | "full")` (`full` = `uniform(0, d(k))`); or a custom policy,
     `retryBackoff(IntToDoubleFunction)` (overrides the scalar knobs; shaped so the core
     `RetryBackoff::delaySeconds` plugs in once the pinned core carries it);
  3. otherwise nothing is released: the reservation is **left alone** and the message is redelivered
     once `retry_after` expires (default 90 s), exactly like the legacy layout (ADR-0034 §9; .NET
     `Leave` parity). A scalar `retryBackoff(0)` base counts as "not set"; use `failureDelaySeconds(0)`
     to release immediately.

  This module has no max-attempts / DLQ-after-N, so a message whose handler always fails is
  redelivered indefinitely: set a backoff (or a delay) and bound the retries on the producer/Laravel
  side (`--tries`). A delay is capped at `RedisConsumer.MAX_FAILURE_DELAY_SECONDS` (30 days);
  `failureDelaySeconds` rejects `NaN`/infinite values and a policy returning `+Infinity` is clamped to
  that cap (a negative or `NaN` policy result counts as 0).

  ```java
  RedisConsumer.builder(redis, "orders")
      .layout(RedisLayout.ZSET)
      .retryBackoff(2).retryBackoffMultiplier(2).retryBackoffMax(60).retryJitter("full")
      .handler("urn:babel:orders:created", (envelope, body) -> process(envelope))
      .build();
  ```

  The `LEGACY` layout is unchanged: a throwing handler leaves the reservation to expire and be
  recovered after `retry_after`; the backoff settings do not apply there.
- **Limit (design A3):** the zset layout stores the bare envelope, so it carries **no**
  `__bq_frame` / `traceparent` headers; trace propagation (below) works in the legacy layout only.
  `trace_id` inside the envelope is still preserved (GR-4).

### Legacy layout: crash recovery

The legacy `<queue>:processing` list is paired with a `<queue>:processing:deadlines` sorted set
(member `<32-hex token>:<raw envelope>`, score `floor(now) + retryAfter`). Before every reserve a
Lua sweep requeues expired reservations byte-for-byte (`attempts` unchanged); ack, release and
dead-letter are gated on removing the deadline member, so a **late ack after a sweep is a
no-op** and never removes a redelivered copy. With `retryAfterDisabled()` a reservation has no
deadline (`+inf`) and is never swept. Default `retryAfterSeconds` is 90.

## Trace propagation (OpenTelemetry `traceparent`, ADR-0028)

Redis lists carry no native metadata channel, so to propagate a W3C `traceparent` (out of
band, beside the frozen envelope — GR-1) the transport wraps the bare envelope in a small,
transport-owned JSON frame distinct from the wire envelope:
`{"__bq_frame":1,"headers":…,"body":<raw envelope>}` (the same `__bq_frame` Go and PHP
use). The optional core `com.babelqueue.otel` module drives it:

```java
// produce: HeaderSender -> RedisPublisher.publishWithHeaders (frames only when headers exist)
RedisPublisher publisher = RedisPublisher.create(redis, "orders");
Tracing.publish(tracer, "urn:babel:orders:created", Map.of("order_id", 1042), "orders",
    (envelope, headers) -> publisher.publishWithHeaders(envelope, headers));

// consume: a header-aware handler receives the surfaced headers for wrapHandler's Supplier
RedisConsumer.builder(redis, "orders")
    .handler("urn:babel:orders:created", (env, body, headers) ->
        Tracing.wrapHandler(tracer, h, () -> headers).handle(env))
    .build();
```

A header-less `publish(...)` stores the bare envelope **byte-for-byte**; a bare
(un-framed) value still consumes — the consumer detects a frame by the reserved
`__bq_frame` sentinel (a frozen envelope never carries it), and the `LREM` ack handle
stays the stored value, so cross-version queues interoperate. The existing
`(env, body)` handler keeps working. Requires `babelqueue-core` ≥ 1.5.0 (this release builds on 1.11.0); no new runtime
dependency (the frame codec is self-contained, the seam is a plain `Map<String,String>`).

## Build & test

```bash
mvn verify
```

Unit tests mock the `RedisCommands` seam (no Redis, no network) with Mockito. The cross-SDK
conformance suite's `redis_layout` / `redis_delay` behavioural cases (Lua scripts, sweep, delayed
window) need a live Redis: set `BQ_REDIS_URL` (default `redis://localhost:6379`). Without Redis
they are skipped; with `BQ_REQUIRE_REDIS=1` (as in CI, a `redis:7` service) an unreachable Redis
fails the build. The pure cases (key names, the delayed-score formula) always run. JaCoCo gates
the build at ≥90% line coverage.

## License

MIT
