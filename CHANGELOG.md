# Changelog

All notable changes to `com.babelqueue:babelqueue-redis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **Laravel-compatible zset layout (`RedisLayout.ZSET`, ADR-0033, conformance 1.6.0).** Keys
  `queues:<q>`, `:delayed`, `:reserved`, `:notify`; reserve / release / migrate / dead-letter are
  single Lua scripts run through `EVALSHA` with an `EVAL` fallback on `NOSCRIPT` (`LuaExecutor`);
  both `cjson` calls of the reserve script (decode and re-encode) are `pcall`-guarded, so a body that
  cannot be decoded or re-encoded (e.g. `1e400`, `NaN`) takes the poison path (stored raw in
  `:reserved`, then dead-lettered) instead of failing the script after the pop. `RedisPublisher.publishDelayed` scores
  `floor(now) + ceil(delay)`. Limit: no `__bq_frame` / `traceparent` in this layout (ADR-0033 §5).
- **Legacy `:processing` sweep.** `<q>:processing:deadlines` sorted set (member
  `<32-hex token>:<raw>`), a Lua sweep before every reserve, and ack/release/DLQ gated on the
  deadline removal (a late ack is a no-op). `retryAfterSeconds` (default 90) /
  `retryAfterDisabled()` / `migrationBatchSize` on the consumer builder.
- Conformance runner for the `redis_layout` and `redis_delay` sections (pure cases always run;
  behavioural cases run against a live Redis, `BQ_REQUIRE_REDIS` turns an unreachable Redis into a
  failure). CI and release run against a `redis:7` service container. The runner writes a
  `results.json` (ADR-0032) when `BQ_CONFORMANCE_RESULTS` is set, which CI validates.

### Changed
- Depends on `babelqueue-core` 1.11.0 (was 1.5.0). The envelope is unchanged (`schema_version: 1`).
- In the default `LEGACY` layout a non-conformant envelope (valid JSON that fails `EnvelopeCodec.accepts`) is
  now dead-lettered to `<queue>.dlq` (raw body, no `dead_letter` block). Before, it was left in
  `<queue>:processing`; with the new deadline sweep it would have been redelivered forever.

## [1.1.0] - 2026-06-21

### Added
- **OpenTelemetry `traceparent` transport wiring (ADR-0028, v0.2).** Redis lists have no
  native per-message metadata channel and the reliable-queue stores the bare body (the
  `LREM` ack handle *is* the stored value), so to carry an out-of-band header (e.g. a W3C
  `traceparent`) beside the frozen envelope the transport now uses a self-contained,
  transport-owned JSON frame distinct from the wire envelope:
  `{"__bq_frame":1,"headers":…,"body":<raw envelope>}` (`RedisFrame`, mirroring the Go/PHP
  `__bq_frame`). `RedisPublisher.publishWithHeaders(Envelope, Map<String,String>)` is the
  produce-side seam the optional core `com.babelqueue.otel.HeaderSender` wires to: with
  usable headers it `RPUSH`es the frame; with none it `RPUSH`es the bare envelope
  byte-for-byte (back-compat). `RedisConsumer` transparently unframes a reserved value —
  bare-value back-compat detection via the reserved `__bq_frame` sentinel (a frozen
  envelope never carries it), the `LREM` ack handle stays the stored value — and surfaces
  the headers to a new header-aware `RedisConsumer.HeaderHandler` (`(envelope, body,
  headers)`), the consume-side seam for `Tracing.wrapHandler(tracer, handler, Supplier)`.
  The existing `(envelope, body)` `BabelHandler` registration is unchanged. The wire
  envelope is never touched (GR-1); `trace_id` is preserved (GR-4); `schema_version` stays
  **1**. No new runtime dependency — the frame codec is self-contained and the header seam
  is a plain `Map<String,String>` (GR-7).

### Changed
- Require `com.babelqueue:babelqueue-core 1.5.0` (the out-of-band header-carrier seam).

## [1.0.0] - 2026-06-14

### Added
- Initial release. A Redis transport on `babelqueue-core` + the Lettuce client,
  implementing §1 of the broker-bindings contract (the reliable-queue list pattern):
  `RedisPublisher` (produce = `RPUSH` the canonical envelope JSON byte-for-byte, no
  wrapping and no property projection — Redis lists carry no native metadata) and
  `RedisConsumer` (reserve = `BLMOVE <queue> <queue>:processing LEFT RIGHT` so an
  in-flight message survives a worker crash → URN-routed `BabelHandler`s → ack via
  `LREM`; at-least-once, a throwing handler leaves the message on the processing list;
  `onError`/`onUnknownUrn` hooks; `attempts` taken from the body unchanged since Redis
  has no native delivery counter). Java 17, JUnit 5, Mockito, JaCoCo ≥90% line coverage
  (currently 100%); unit tests mock the `RedisCommands` seam (no Redis, no network) and
  capture the `RPUSH`/`BLMOVE`/`LREM` calls. The envelope is unchanged
  (`schema_version: 1`); Redis is purely additive. This is a Java-owned reliable queue;
  full parity with Laravel's reserved-sorted-set reservation on a shared Redis queue is
  a separate task (broker-bindings §1.4).

[1.1.0]: https://github.com/BabelQueue/babelqueue-java-redis/releases/tag/v1.1.0
[1.0.0]: https://github.com/BabelQueue/babelqueue-java-redis/releases/tag/v1.0.0
