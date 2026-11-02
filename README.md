# Redelay

Redis backed delay queue: publish an event with an optional delay, a ticker moves due
events into a per-key buffer list, consumers claim them into an `unAck` zset, run the
handler and ack. A scanner requeues events whose handler died before acking.

## usage

```go
import "Redelay"

svr := Redelay.NewService("sample:svc", redisClient)

key := "sample:job"

// LaunchJob blocks and drains on cancel: run one per key, one goroutine each.
go svr.LaunchJob(ctx, key, Redelay.NewJob(handler))

// publish now, or publish with a delay in seconds (delay is second precision)
msg := &Redelay.Message[Payload]{Data: payload}
if err := svr.Publish(ctx, key, msg.Marshall()); err != nil {
    // redis failed, the event was not queued
}
if err := svr.PublishDelay(ctx, key, msg.Marshall(), 60); err != nil {
    // redis failed, the event was not queued
}
```

Plain `Job` handlers work too, they get the raw args string:

```go
go svr.LaunchJob(ctx, key, func(args string) error {
    return nil
})
```

`ctx` decides when the consumer loop stops; see shutdown below.

`Message[T].Marshall()` fills in a `msgId` when empty, so the same message keeps a
stable id across its retries.

```go
func handler(ctx context.Context, p Payload) error {
    // ctx carries opId = msgId and expires after DELAYTASK_JOB_TIMEOUT_SEC,
    // the same value as the jobTimeout gate that acks a stuck handler
    if transient(p) {
        return Redelay.RetryErr // requeue with backoff
    }
    return fmt.Errorf("rejected") // any other error is acked, no retry
}
```

## shutdown

Cancelling the ctx passed to `LaunchJob` stops it from claiming new events, then it waits
for the handlers already in flight and returns. Those handlers keep a detached ctx, so
their ack still lands and their events are not requeued: a clean shutdown does not replay
business logic. The wait is bounded by the `jobTimeout` gate, so a hung handler adds at
most one `jobTimeout`. Set `terminationGracePeriodSeconds` higher than `jobTimeout`
plus a margin, otherwise the SIGKILL arrives first and the in-flight events do get
requeued by the unAck scanner:

```yaml
terminationGracePeriodSeconds: 90 # >= DELAYTASK_JOB_TIMEOUT_SEC (default 60) + margin
```

## delivery semantics

| outcome | what happens |
| --- | --- |
| handler returns `nil` | acked once |
| handler returns `RetryErr` | requeued after `3 * retryCount` seconds, dropped after 3 retries |
| handler returns any other error | acked anyway, counted as `job_failed_total` |
| handler panics | counted as a business error, acked anyway |
| handler runs longer than `jobTimeout` | acked anyway, counted as `job_timeout_total`; the goroutine keeps running, a raw `Job` has no context to cancel it |
| another instance holds the event lock | skipped, not acked, counted as `duplicate_exec_total` |
| the event lock cannot be acquired (redis error) | requeued like `RetryErr`, never silently dropped |
| event stuck in `unAck` longer than `unAckTimeout` | requeued, dropped after 3 requeues |
| member that is not a valid event json | dropped where it is found: `malformed_event_total` in the delay queue, `malformed_unAck_event_total{key}` in `unAck` |

Events are distributed by an `id` lock with `ttl = jobTimeout`, and the queue moves are
lua scripts, so N instances consume each event once. Publish twice with identical args
and you get two deliveries: `msgId` keeps them apart.

## configuration

| env | default | meaning |
| --- | --- | --- |
| `DELAYTASK_TICK_COUNT` | `500` | events per tick, also the per-scan cap of the unAck scanner |
| `DELAYTASK_TICK_INTERVAL_MS` | `500` | ticker period, and the consumer idle poll interval |
| `DELAYTASK_CONSUME_LIMIT` | `tickCount * 5` | consumer rate limit per second, burst is twice that |
| `DELAYTASK_JOB_TIMEOUT_SEC` | `60` | hard timeout of a single handler run, the event lock ttl, the drain bound at shutdown, and the deadline of the ctx `NewJob` hands to your handler |
| `DELAYTASK_UNACK_TIMEOUT_SEC` | `180` | how long an event may sit unacked; forced to `jobTimeout * 2` if not greater |
| `DELAYTASK_REDIS_CLUSTER` | unset | adds a `{svcName:Redelay}` hash tag so the scripts stay in one slot |
| `REDELAY_DEBUG` | unset | verbose ticker logs (`true` / `TRUE` / `1` / `yes` / `YES`) |

`DELAYTASK_REDIS_CLUSTER` changes the key names, so existing data is not picked up
after switching it on.

## keys

```
<svc>:Redelay                        delay zset, score = run at (unix seconds)
<svc>:Redelay:bufferQueue:<key>      due events waiting for a consumer
<svc>:Redelay:unAck:<key>            events claimed by a consumer, score = claimed at
<svc>:Redelay:runEvent:lock:<id>     per event execution lock
<svc>:tickQ:tick:lock                cross instance tick rate limit
<svc>:tickUnAckQ:tick:lock:<key>     cross instance unAck scan rate limit
```

## metrics

Registered on the prometheus default registry with const labels
`service` / `type=delay-task` / `hostname`. A collector that already exists is reused,
so several services in one process are safe; a registration error is logged, never
panicked. Do not use one metric name for both a counter and a gauge: `*prometheus.Gauge`
also satisfies the counter interface, so such a reuse would write into the gauge.

```
publish_total  consume_total{key}  tickQ_tick_total  tickUnAckQ_tick_total{key}
job_failed_total{key}  job_timeout_total{key}  duplicate_exec_total{key}
malformed_event_total  malformed_unAck_event_total{key}
delay_queue_length  buffer_queue_length{key}  unAck_queue_length{key}
redis_ping  tick_ping  tick_unack_ping{key}  consume_ping{key}  loop_flush_exp_ping
tick_count  tick_interval_ms  consume_limit
```

Serve them yourself:

```go
E := gin.New()
E.GET("/metrics", gin.WrapH(promhttp.Handler()))
```

## tests

```bash
# a redis is required, 127.0.0.1:6379 by default
docker run -d --name redelay-test-redis -p 16379:6379 redis:7-alpine

FILTER='TestGetLogger|TestCtxLogger|TestNewService|TestDebug_Switch|TestTryRun|TestStartLoopGuards|TestEnvSnapshot|TestMessage|TestEvent|TestIntegration'
REDELAY_REDIS_ADDR=127.0.0.1:16379 go test -count=1 -timeout 900s -run "$FILTER" .

# skip the slow cases (retry backoff, two instances, rate limit, malformed buffer member, metrics loop)
REDELAY_REDIS_ADDR=127.0.0.1:16379 go test -count=1 -short -timeout 300s -run "$FILTER" .

# the exporter package needs no redis
go test -count=1 ./exporter/
```

Tests clean up their own keys (`redelay-it-<testname>*`) and skip when redis is missing.
`TestService` in `service_test.go` is the original manual playground case: it publishes
about 1000 events one second apart against a hardcoded test instance and therefore runs
far longer than the default `go test` timeout, which is why the commands above select the
suite with `-run` instead of running `./...` unfiltered.
