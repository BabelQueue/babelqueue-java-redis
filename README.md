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
  <version>1.0.0</version>
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

Retry is **at-least-once**: a throwing handler leaves the message on the
`<queue>:processing` list (a recovery sweep can requeue it); a successful handler `LREM`s
it. The poll loop never stops on a bad message — observe via `onError` / `onUnknownUrn`.
The envelope is unchanged (`schema_version` stays `1`); Redis is purely additive.

> **Scope.** This is a **Java-owned reliable queue**, mirroring the Go runtime's
> reliable-queue mechanism. Pointed at a queue this SDK owns end-to-end it is a complete,
> crash-safe transport. **Full parity with Laravel's reserved-sorted-set reservation on a
> _shared_ PHP+Java Redis queue is a separate task** (see broker-bindings §1.4): a
> consumer reading a queue produced by the Laravel driver must replicate Laravel's
> reserve/ack semantics.

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
`(env, body)` handler keeps working. Requires `babelqueue-core` ≥ 1.5.0; no new runtime
dependency (the frame codec is self-contained, the seam is a plain `Map<String,String>`).

## Build & test

```bash
mvn verify
```

Unit tests mock the `RedisCommands` seam (no Redis, no network) and capture the
`RPUSH`/`BLMOVE`/`LREM` calls with Mockito. JaCoCo gates the build at ≥90% line coverage.

## License

MIT
