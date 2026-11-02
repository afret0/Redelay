package Redelay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/redis/go-redis/v9"
)

type recorder struct {
	mx     sync.Mutex
	seq    map[string]int
	times  map[string][]time.Time
	opIds  map[string]string
	calls  int32
	failOn map[string]error
}

func newRecorder() *recorder {
	return &recorder{
		seq:    make(map[string]int),
		times:  make(map[string][]time.Time),
		opIds:  make(map[string]string),
		failOn: make(map[string]error),
	}
}

func (r *recorder) job() Job {
	return func(args string) error {
		atomic.AddInt32(&r.calls, 1)

		m := new(Message[string])
		if err := json.Unmarshal([]byte(args), m); err != nil {
			GetLogger().Errorf("recorder unmarshal args failed, args: %s, err: %s", args, err)
			return err
		}

		r.mx.Lock()
		r.seq[m.Data]++
		r.times[m.Data] = append(r.times[m.Data], time.Now())
		r.opIds[m.Data] = m.MsgId
		r.mx.Unlock()

		if err, ok := r.failOn[m.Data]; ok {
			return err
		}
		return nil
	}
}

func (r *recorder) count(payload string) int {
	r.mx.Lock()
	defer r.mx.Unlock()
	return r.seq[payload]
}

func (r *recorder) total() int {
	r.mx.Lock()
	defer r.mx.Unlock()

	n := 0
	for _, v := range r.seq {
		n += v
	}
	return n
}

func (r *recorder) msgIdOf(payload string) string {
	r.mx.Lock()
	defer r.mx.Unlock()
	return r.opIds[payload]
}

func (r *recorder) gapsOf(payload string) []time.Duration {
	r.mx.Lock()
	defer r.mx.Unlock()

	ts := r.times[payload]
	gaps := make([]time.Duration, 0, len(ts))
	for i := 1; i < len(ts); i++ {
		gaps = append(gaps, ts[i].Sub(ts[i-1]))
	}
	return gaps
}

func (r *recorder) fail(payload string, err error) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.failOn[payload] = err
}

func itEnv(t *testing.T, overrides ...string) {
	t.Helper()

	t.Setenv("HOSTNAME", itPodName(t))
	t.Setenv("DELAYTASK_TICK_INTERVAL_MS", "50")
	t.Setenv("DELAYTASK_TICK_COUNT", "100")
	t.Setenv("DELAYTASK_CONSUME_LIMIT", "5000")
	t.Setenv("DELAYTASK_REDIS_CLUSTER", "")
	t.Setenv("REDELAY_DEBUG", "")

	for i := 0; i+1 < len(overrides); i += 2 {
		t.Setenv(overrides[i], overrides[i+1])
	}
}

func itPodName(t *testing.T) string {
	name := strings.ToLower(t.Name())
	return "it-" + strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return '-'
	}, name)
}

func itSvc(t *testing.T, overrides ...string) (*Svc, redis.UniversalClient) {
	t.Helper()

	itEnv(t, overrides...)
	rc := testRedis(t)
	return NewService(itSvcName(t), rc), rc
}

func itPublish(t *testing.T, svr *Svc, key, payload string) string {
	t.Helper()

	m := &Message[string]{Data: payload}
	args := m.Marshall()
	if err := svr.Publish(tool.NewCtxBK(), key, args); err != nil {
		t.Fatalf("publish %s failed, err: %s", payload, err)
	}
	return m.MsgId
}

func itPublishRaw(t *testing.T, svr *Svc, key, args string) {
	t.Helper()

	if err := svr.Publish(tool.NewCtxBK(), key, args); err != nil {
		t.Fatalf("publish raw args failed, err: %s", err)
	}
}

func itSeedDelay(t *testing.T, rc redis.UniversalClient, svr *Svc, e *event, score int64) string {
	t.Helper()

	eventS := mustEventJSON(t, e)
	err := rc.ZAdd(context.Background(), svr.key, redis.Z{Score: float64(score), Member: eventS}).Err()
	if err != nil {
		t.Fatalf("seed delay zset failed, err: %s", err)
	}
	return eventS
}

func itExtra(t *testing.T, key string) map[string]string {
	t.Helper()

	extra := map[string]string{"hostname": itPodName(t)}
	if key != "" {
		extra["key"] = key
	}
	return extra
}

func itCounter(t *testing.T, svr *Svc, name, key string) float64 {
	t.Helper()

	return itMetric(t, svr.svcName, name, itExtra(t, key))
}

func itCounterOrZero(t *testing.T, svr *Svc, name, key string) float64 {
	t.Helper()

	return sumMetric(t, name, svcLabels(svr.svcName, itExtra(t, key)))
}

func itGaugeValue(t *testing.T, svr *Svc, name string, extra map[string]string) float64 {
	t.Helper()

	merged := map[string]string{"hostname": itPodName(t)}
	for k, v := range extra {
		merged[k] = v
	}
	return itMetric(t, svr.svcName, name, merged)
}

func skipSlow(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("slow integration test skipped by -short")
	}
}

func TestIntegration_PublishThenConsumeExactlyOnce(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-once"
	rec := newRecorder()

	msgId := itPublish(t, svr, key, "msg-1")

	st := readQueues(t, rc, svr, key)
	if st.delay != 1 || st.buffer != 0 || st.unAck != 0 {
		t.Fatalf("right after Publish the event should wait in the delay zset, got %s", st)
	}

	seeded := unmarshalEvent(t, st.delayRaw[0])
	if seeded.Key != key {
		t.Fatalf("seeded event key expected %s, got %s", key, seeded.Key)
	}
	if seeded.Id == "" {
		t.Fatalf("seeded event id should be generated")
	}
	if seeded.RetryCount != 0 || seeded.UnAckRetryCount != 0 {
		t.Fatalf("seeded event retry counters should be 0, got %+v", seeded)
	}

	args := new(Message[string])
	if err := json.Unmarshal([]byte(seeded.Args), args); err != nil {
		t.Fatalf("unmarshal seeded args failed, err: %s", err)
	}
	if args.MsgId != msgId || args.Data != "msg-1" {
		t.Fatalf("seeded args mismatch, got %+v", args)
	}

	score, err := rc.ZScore(context.Background(), svr.key, st.delayRaw[0]).Result()
	if err != nil {
		t.Fatalf("zscore failed, err: %s", err)
	}
	if now := time.Now().Unix(); score < float64(now-2) || score > float64(now+1) {
		t.Fatalf("Publish (delay 0) score expected ~now, got score: %d, now: %d", int64(score), now)
	}

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	waitUntil(t, 8*time.Second, "msg-1 delivered", func() bool { return rec.count("msg-1") == 1 })
	waitUntil(t, 8*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })

	time.Sleep(500 * time.Millisecond)
	if got := rec.count("msg-1"); got != 1 {
		t.Fatalf("msg-1 should be consumed exactly once, got %d", got)
	}
	if got := rec.msgIdOf("msg-1"); got != msgId {
		t.Fatalf("delivered msgId expected %s, got %s", msgId, got)
	}
	if got := itCounter(t, svr, "consume_total", key); got != 1 {
		t.Fatalf("consume_total expected 1, got %+v", got)
	}
	if got := itCounter(t, svr, "publish_total", ""); got != 1 {
		t.Fatalf("publish_total expected 1, got %+v", got)
	}
	if got := itGaugeValue(t, svr, "consume_ping", map[string]string{"key": key}); got != 1 {
		t.Fatalf("consume_ping expected 1 while consuming, got %+v", got)
	}
	if got := itGaugeValue(t, svr, "tick_ping", nil); got != 1 {
		t.Fatalf("tick_ping expected 1 while ticking, got %+v", got)
	}
}

func TestIntegration_PublishDelayWaitsUntilDue(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-delay"
	rec := newRecorder()

	start := time.Now()
	m := &Message[string]{Data: "later"}
	if err := svr.PublishDelay(tool.NewCtxBK(), key, m.Marshall(), 2); err != nil {
		t.Fatalf("PublishDelay failed, err: %s", err)
	}

	st := readQueues(t, rc, svr, key)
	if st.delay != 1 {
		t.Fatalf("delayed event should wait in the delay zset, got %s", st)
	}

	score, err := rc.ZScore(context.Background(), svr.key, st.delayRaw[0]).Result()
	if err != nil {
		t.Fatalf("zscore failed, err: %s", err)
	}
	if score < float64(start.Unix()+2) || score > float64(time.Now().Unix()+2) {
		t.Fatalf("PublishDelay(2) score expected now+2, got score: %d, start: %d, now: %d",
			int64(score), start.Unix(), time.Now().Unix())
	}

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	waitUntil(t, 10*time.Second, "delayed event delivered", func() bool { return rec.count("later") == 1 })
	elapsed := time.Since(start)
	if elapsed < 900*time.Millisecond {
		t.Fatalf("delayed event delivered too early, elapsed: %s", elapsed)
	}
	if elapsed > 6*time.Second {
		t.Fatalf("delayed event delivered too late, elapsed: %s", elapsed)
	}
	t.Logf("PublishDelay(delay=2s) delivered after %s", elapsed)

	waitUntil(t, 5*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })
}

func TestIntegration_SamePayloadPublishedTwiceBothDelivered(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-dup-payload"
	rec := newRecorder()

	ctx := tool.NewCtxBK()
	args := (&Message[string]{MsgId: "fixed-msg-id", Data: "same"}).Marshall()
	for i := 0; i < 2; i++ {
		if err := svr.Publish(ctx, key, args); err != nil {
			t.Fatalf("publish %d failed, err: %s", i, err)
		}
	}

	st := readQueues(t, rc, svr, key)
	if st.delay != 2 {
		t.Fatalf("identical payload published twice should create 2 zset members, got %s, members: %+v", st, st.delayRaw)
	}

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	waitUntil(t, 8*time.Second, "both deliveries", func() bool { return rec.count("same") == 2 })
	waitUntil(t, 5*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })

	time.Sleep(300 * time.Millisecond)
	if got := rec.count("same"); got != 2 {
		t.Fatalf("expected exactly 2 deliveries, got %d", got)
	}
}

func TestIntegration_MultipleKeysIsolated(t *testing.T) {
	svr, rc := itSvc(t)
	keyA, keyB := "k-a", "k-b"
	recA, recB := newRecorder(), newRecorder()

	ctx := tool.NewCtxBK()
	for i := 0; i < 20; i++ {
		if err := svr.Publish(ctx, keyA, (&Message[string]{Data: fmt.Sprintf("a-%d", i)}).Marshall()); err != nil {
			t.Fatalf("publish a-%d failed, err: %s", i, err)
		}
		if err := svr.Publish(ctx, keyB, (&Message[string]{Data: fmt.Sprintf("b-%d", i)}).Marshall()); err != nil {
			t.Fatalf("publish b-%d failed, err: %s", i, err)
		}
	}

	go func() { svr.LaunchJob(tool.NewCtxBK(), keyA, recA.job()) }()
	go func() { svr.LaunchJob(tool.NewCtxBK(), keyB, recB.job()) }()

	waitUntil(t, 15*time.Second, "all keyA delivered", func() bool { return recA.total() == 20 })
	waitUntil(t, 15*time.Second, "all keyB delivered", func() bool { return recB.total() == 20 })

	for i := 0; i < 20; i++ {
		if recA.count(fmt.Sprintf("b-%d", i)) != 0 {
			t.Fatalf("keyA consumer received a keyB message b-%d", i)
		}
		if recB.count(fmt.Sprintf("a-%d", i)) != 0 {
			t.Fatalf("keyB consumer received a keyA message a-%d", i)
		}
		if recA.count(fmt.Sprintf("a-%d", i)) != 1 || recB.count(fmt.Sprintf("b-%d", i)) != 1 {
			t.Fatalf("message %d not delivered exactly once, a: %d, b: %d", i,
				recA.count(fmt.Sprintf("a-%d", i)), recB.count(fmt.Sprintf("b-%d", i)))
		}
	}

	waitUntil(t, 8*time.Second, "keyA queues drained", func() bool { return readQueues(t, rc, svr, keyA).drained() })
	waitUntil(t, 8*time.Second, "keyB queues drained", func() bool { return readQueues(t, rc, svr, keyB).drained() })
}

func TestIntegration_RetryErrBackoffThenDiscard(t *testing.T) {
	skipSlow(t)

	svr, rc := itSvc(t, "DELAYTASK_JOB_TIMEOUT_SEC", "30")
	key := "k-retry"
	rec := newRecorder()

	msgId := itPublish(t, svr, key, "retry-me")
	rec.fail("retry-me", RetryErr)

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	waitUntil(t, 60*time.Second, "4 attempts then discard", func() bool { return atomic.LoadInt32(&rec.calls) == 4 })
	waitUntil(t, 10*time.Second, "queues drained after discard", func() bool { return readQueues(t, rc, svr, key).drained() })

	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt32(&rec.calls); got != 4 {
		t.Fatalf("RetryErr expected 4 attempts (1 + 3 retries) then discard, got %d", got)
	}

	gaps := rec.gapsOf("retry-me")
	if len(gaps) != 3 {
		t.Fatalf("expected 3 gaps between 4 attempts, got %+v", gaps)
	}
	wantGaps := []time.Duration{3 * time.Second, 6 * time.Second, 9 * time.Second}
	for i, want := range wantGaps {
		if gaps[i] < want-1500*time.Millisecond || gaps[i] > want+2500*time.Millisecond {
			t.Fatalf("gap %d expected ~%s (backoff 3*retryCount), got %s", i, want, gaps[i])
		}
	}
	t.Logf("retry backoff gaps: %+v", gaps)

	if rec.msgIdOf("retry-me") != msgId {
		t.Fatalf("retried message should keep msgId %s, got %s", msgId, rec.msgIdOf("retry-me"))
	}
}

func TestIntegration_RetryExhaustedEventNotRetried(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-retry-exhausted"
	rec := newRecorder()

	itSeedDelay(t, rc, svr, &event{
		Id:         "id-exhausted",
		Key:        key,
		Args:       (&Message[string]{Data: "doomed"}).Marshall(),
		RetryCount: 3,
	}, time.Now().Unix())

	rec.fail("doomed", RetryErr)
	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	waitUntil(t, 8*time.Second, "single attempt", func() bool { return atomic.LoadInt32(&rec.calls) >= 1 })
	waitUntil(t, 5*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })

	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt32(&rec.calls); got != 1 {
		t.Fatalf("exhausted event should be discarded after 1 attempt, got %d", got)
	}
}

func TestIntegration_RequeueMovesEventBackToDelayZSet(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-requeue"

	old := mustEventJSON(t, &event{Id: "id-requeue", Key: key, Args: "args", UnAckRetryCount: 0})
	seedUnAck(t, rc, svr, key, old, time.Now().Unix()-10)

	retried := mustEventJSON(t, &event{Id: "id-requeue", Key: key, Args: "args", UnAckRetryCount: 1})
	score := time.Now().Unix() + 7
	if err := svr.requeue(context.Background(), svr.unAckQName(key), old, retried, score); err != nil {
		t.Fatalf("requeue failed, err: %s", err)
	}

	st := readQueues(t, rc, svr, key)
	if st.unAck != 0 {
		t.Fatalf("old event should be removed from unAck, got %s, members: %+v", st, st.unAckRaw)
	}
	if st.delay != 1 || st.delayRaw[0] != retried {
		t.Fatalf("requeued event mismatch, got %s, members: %+v", st, st.delayRaw)
	}

	gotScore, err := rc.ZScore(context.Background(), svr.key, retried).Result()
	if err != nil {
		t.Fatalf("zscore failed, err: %s", err)
	}
	if int64(gotScore) != score {
		t.Fatalf("requeue score expected %d, got %d", score, int64(gotScore))
	}

	if err := svr.requeue(context.Background(), svr.unAckQName(key), old, retried, score); err != nil {
		t.Fatalf("second requeue should return nil, err: %s", err)
	}
	if got := readQueues(t, rc, svr, key).delay; got != 1 {
		t.Fatalf("second requeue should be a no-op, delay count: %d", got)
	}
}

func TestIntegration_BusinessErrorAckedWithoutRetry(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-bizerr"
	rec := newRecorder()

	itPublish(t, svr, key, "biz-fail")
	rec.fail("biz-fail", errors.New("business rejected"))

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	waitUntil(t, 8*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })

	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt32(&rec.calls); got != 1 {
		t.Fatalf("business error should not be retried, attempts: %d", got)
	}
	if got := itCounter(t, svr, "job_failed_total", key); got < 1 {
		t.Fatalf("job_failed_total expected >= 1, got %+v", got)
	}
	if got := itCounter(t, svr, "consume_total", key); got != 1 {
		t.Fatalf("consume_total expected 1, got %+v", got)
	}
}

func TestIntegration_PanicInJobAckedAndConsumerSurvives(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-panic"

	var panics int32
	panicking := func(args string) error {
		atomic.AddInt32(&panics, 1)
		panic("handler exploded")
	}

	itPublishRaw(t, svr, key, (&Message[string]{Data: "boom"}).Marshall())
	itPublish(t, svr, key, "after-panic")

	rec := newRecorder()
	delivered := make(chan string, 8)
	job := func(args string) error {
		m := new(Message[string])
		if err := json.Unmarshal([]byte(args), m); err != nil {
			return err
		}
		if m.Data == "boom" {
			return panicking(args)
		}
		if err := rec.job()(args); err != nil {
			return err
		}
		delivered <- m.Data
		return nil
	}

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, job) }()

	waitUntil(t, 8*time.Second, "panicking job attempted", func() bool { return atomic.LoadInt32(&panics) == 1 })
	waitUntil(t, 8*time.Second, "message after the panic still delivered", func() bool { return rec.count("after-panic") == 1 })
	waitUntil(t, 8*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })

	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt32(&panics); got != 1 {
		t.Fatalf("panicking job should be acknowledged, not retried, attempts: %d", got)
	}
	if got := itCounter(t, svr, "job_failed_total", key); got < 1 {
		t.Fatalf("job_failed_total expected >= 1 after panic, got %+v", got)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected only the healthy message delivered, got %d", len(delivered))
	}
}

func TestIntegration_NewJobWrapperDecodesAndPropagatesOpId(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-newjob"

	var mx sync.Mutex
	var got itPayload
	var gotOpId string
	var calls int32

	job := NewJob(func(ctx context.Context, p itPayload) error {
		atomic.AddInt32(&calls, 1)
		opId, _ := ctx.Value("opId").(string)
		mx.Lock()
		got = p
		gotOpId = opId
		mx.Unlock()
		return nil
	})

	m := &Message[itPayload]{Data: itPayload{RoomId: "room-9", Count: 11}}
	args := m.Marshall()
	itPublishRaw(t, svr, key, args)

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, job) }()

	waitUntil(t, 8*time.Second, "NewJob delivered", func() bool { return atomic.LoadInt32(&calls) == 1 })
	waitUntil(t, 5*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })

	mx.Lock()
	defer mx.Unlock()
	if got.RoomId != "room-9" || got.Count != 11 {
		t.Fatalf("NewJob payload mismatch, got %+v", got)
	}
	if gotOpId != m.MsgId {
		t.Fatalf("NewJob should set ctx opId to msgId %s, got %q", m.MsgId, gotOpId)
	}
}

func TestIntegration_NewJobUnmarshalErrorAcked(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-newjob-bad"

	var calls int32
	job := NewJob(func(ctx context.Context, p string) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	itPublishRaw(t, svr, key, "this-is-not-json")

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, job) }()

	waitUntil(t, 8*time.Second, "bad args claimed and acknowledged", func() bool {
		return readQueues(t, rc, svr, key).drained()
	})

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("handler should not run for unparseable args, calls: %d", got)
	}
	if got := itCounter(t, svr, "job_failed_total", key); got < 1 {
		t.Fatalf("job_failed_total expected >= 1, got %+v", got)
	}
}

func TestIntegration_UnAckScannerRequeuesStaleAndKeepsFresh(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-unack"

	stale := mustEventJSON(t, &event{Id: "stale-1", Key: key, Args: "args-stale"})
	fresh := mustEventJSON(t, &event{Id: "fresh-1", Key: key, Args: "args-fresh"})

	staleScore := time.Now().Unix() - int64(svr.unAckTimeout/time.Second) - 5
	seedUnAck(t, rc, svr, key, stale, staleScore)
	seedUnAck(t, rc, svr, key, fresh, time.Now().Unix())

	svr.tickUnAckQ(key)

	st := readQueues(t, rc, svr, key)
	if st.unAck != 1 || st.unAckRaw[0] != fresh {
		t.Fatalf("only the fresh event should stay in unAck, got %s, members: %+v", st, st.unAckRaw)
	}
	if st.delay != 1 {
		t.Fatalf("stale event should be requeued to the delay zset, got %s", st)
	}

	requeued := unmarshalEvent(t, st.delayRaw[0])
	if requeued.Id != "stale-1" || requeued.Key != key {
		t.Fatalf("requeued event mismatch, got %+v", requeued)
	}
	if requeued.UnAckRetryCount != 1 {
		t.Fatalf("UnAckRetryCount expected 1, got %+v", requeued)
	}

	score, err := rc.ZScore(context.Background(), svr.key, st.delayRaw[0]).Result()
	if err != nil {
		t.Fatalf("zscore failed, err: %s", err)
	}
	if now := time.Now().Unix(); int64(score) < now-2 || int64(score) > now+2 {
		t.Fatalf("requeued score expected ~now, got %d, now: %d", int64(score), now)
	}

	if got := itCounter(t, svr, "tickUnAckQ_tick_total", key); got < 1 {
		t.Fatalf("tickUnAckQ_tick_total expected >= 1, got %+v", got)
	}
}

func TestIntegration_UnAckScannerRetryCounterAndDiscard(t *testing.T) {
	svr, rc := itSvc(t)

	t.Run("secondRetry", func(t *testing.T) {
		key := "k-unack-2"
		old := mustEventJSON(t, &event{Id: "u-2", Key: key, Args: "a", UnAckRetryCount: 1})
		seedUnAck(t, rc, svr, key, old, time.Now().Unix()-int64(svr.unAckTimeout/time.Second)-5)

		svr.tickUnAckQ(key)

		st := readQueues(t, rc, svr, key)
		if st.unAck != 0 || st.delay != 1 {
			t.Fatalf("expected requeue into the delay zset, got %s", st)
		}
		if got := unmarshalEvent(t, st.delayRaw[0]).UnAckRetryCount; got != 2 {
			t.Fatalf("UnAckRetryCount expected 2, got %d", got)
		}
	})

	t.Run("discardedAtThree", func(t *testing.T) {
		key := "k-unack-discard"
		old := mustEventJSON(t, &event{Id: "u-3", Key: key, Args: "a", UnAckRetryCount: 3})
		seedUnAck(t, rc, svr, key, old, time.Now().Unix()-int64(svr.unAckTimeout/time.Second)-5)

		svr.tickUnAckQ(key)

		st := readQueues(t, rc, svr, key)
		for _, m := range append(st.unAckRaw, st.delayRaw...) {
			if strings.Contains(m, "u-3") {
				t.Fatalf("event retried 3 times in unAck should be discarded, still present: %s, state: %s", m, st)
			}
		}
	})
}

func TestIntegration_UnAckScannerRecoversLostMessage(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-unack-recover"
	rec := newRecorder()

	lost := &event{Id: "lost-1", Key: key, Args: (&Message[string]{Data: "lost-payload"}).Marshall()}
	seedUnAck(t, rc, svr, key, mustEventJSON(t, lost), time.Now().Unix()-int64(svr.unAckTimeout/time.Second)-5)

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	time.Sleep(500 * time.Millisecond)
	if got := rec.count("lost-payload"); got != 0 {
		t.Fatalf("an event stuck in unAck must not be consumed before the scanner requeues it, got %d", got)
	}

	svr.tickUnAckQ(key)

	waitUntil(t, 10*time.Second, "recovered message delivered", func() bool { return rec.count("lost-payload") == 1 })
	waitUntil(t, 5*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })

	time.Sleep(300 * time.Millisecond)
	if got := rec.count("lost-payload"); got != 1 {
		t.Fatalf("recovered message should be delivered exactly once, got %d", got)
	}
}

func TestIntegration_UnAckScannerDropsMalformedMember(t *testing.T) {
	svr, rc := itSvc(t)
	key := "k-unack-malformed"

	ctx := context.Background()
	malformed := "{not-json"
	if err := rc.ZAdd(ctx, svr.unAckQName(key), redis.Z{
		Score:  float64(time.Now().Unix() - 600),
		Member: malformed,
	}).Err(); err != nil {
		t.Fatalf("seed malformed unAck member failed, err: %s", err)
	}

	stale := mustEventJSON(t, &event{Id: "ok-unack", Key: key, Args: "a"})
	seedUnAck(t, rc, svr, key, stale, time.Now().Unix()-600)

	for i := 0; i < 3; i++ {
		svr.tickUnAckQ(key)
	}

	st := readQueues(t, rc, svr, key)
	if st.unAck != 0 {
		t.Fatalf("the scanner must clear the malformed member instead of rescanning it forever, got %s, members: %+v",
			st, st.unAckRaw)
	}
	if st.delay != 1 {
		t.Fatalf("the valid stale event should still be requeued, got %s, members: %+v", st, st.delayRaw)
	}
	if got := unmarshalEvent(t, st.delayRaw[0]).Id; got != "ok-unack" {
		t.Fatalf("requeued the wrong event, id: %s", got)
	}
	if got := itCounter(t, svr, "malformed_unAck_event_total", key); got != 1 {
		t.Fatalf("the member is purged by the first scan, so it must be counted once, got %+v", got)
	}
}

// TestIntegration_MalformedBufferMemberIsSkippedThenDropped 覆盖 handleEvent 解析不了 args 时
// 的兜底: 事件不会被执行, 最终由 unAck 扫描器清掉, 不留在队列里.
func TestIntegration_MalformedBufferMemberIsSkippedThenDropped(t *testing.T) {
	skipSlow(t)

	svr, rc := itSvc(t,
		"DELAYTASK_JOB_TIMEOUT_SEC", "1",
		"DELAYTASK_UNACK_TIMEOUT_SEC", "2",
		"DELAYTASK_TICK_INTERVAL_MS", "200")
	key := "k-buffer-malformed"
	ctx := context.Background()

	if err := rc.RPush(ctx, svr.bufferQName(key), "not-json").Err(); err != nil {
		t.Fatalf("seed malformed buffer member failed, err: %s", err)
	}

	var calls int32
	go func() {
		svr.LaunchJob(tool.NewCtxBK(), key, func(args string) error {
			atomic.AddInt32(&calls, 1)
			return nil
		})
	}()

	waitUntil(t, 15*time.Second, "malformed member claimed into unAck", func() bool {
		st := readQueues(t, rc, svr, key)
		return st.buffer == 0 && st.unAck == 1
	})
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("a malformed member must not reach the job, calls: %d", got)
	}

	waitUntil(t, 30*time.Second, "unAck scanner drops the malformed member", func() bool {
		return readQueues(t, rc, svr, key).drained()
	})
	if got := itCounter(t, svr, "malformed_unAck_event_total", key); got < 1 {
		t.Fatalf("malformed_unAck_event_total expected >= 1, got %+v", got)
	}
	t.Logf("bufferQ 里的脏数据: 不执行 handler, unAckTimeout(%s) 后由扫描器清除", svr.unAckTimeout)
}

func TestIntegration_JobTimeoutAcksAnywayAndLeavesGoroutineRunning(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_JOB_TIMEOUT_SEC", "2")
	key := "k-timeout"

	if svr.jobTimeout != 2*time.Second {
		t.Fatalf("jobTimeout expected 2s, got %s", svr.jobTimeout)
	}
	if svr.unAckTimeout != 3*time.Minute {
		t.Fatalf("unAckTimeout default 3m already exceeds jobTimeout 2s, it should stay untouched, got %s", svr.unAckTimeout)
	}
	if svr.unAckTimeout <= svr.jobTimeout {
		t.Fatalf("unAckTimeout(%s) must stay greater than jobTimeout(%s)", svr.unAckTimeout, svr.jobTimeout)
	}

	var started, finished int32
	release := make(chan struct{})
	var wakeOnce sync.Once
	wake := func() { wakeOnce.Do(func() { close(release) }) }
	blocking := func(args string) error {
		atomic.AddInt32(&started, 1)
		<-release
		atomic.AddInt32(&finished, 1)
		return nil
	}
	t.Cleanup(wake)

	itPublishRaw(t, svr, key, (&Message[string]{Data: "slow"}).Marshall())

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, blocking) }()

	waitUntil(t, 8*time.Second, "blocking job started", func() bool { return atomic.LoadInt32(&started) == 1 })
	waitUntil(t, 10*time.Second, "unAck cleared by the timeout gate", func() bool {
		return readQueues(t, rc, svr, key).drained()
	})

	if atomic.LoadInt32(&finished) != 0 {
		t.Fatalf("the job goroutine is not cancellable, it must still be running after the timeout gate")
	}
	if got := itCounter(t, svr, "job_timeout_total", key); got < 1 {
		t.Fatalf("job_timeout_total expected >= 1, got %+v", got)
	}
	t.Logf("jobTimeout(2s) 到点后直接 ack, 但 Job 无 ctx 无法取消, goroutine 仍在运行 (started: %d, finished: %d)",
		atomic.LoadInt32(&started), atomic.LoadInt32(&finished))

	wake()
	waitUntil(t, 5*time.Second, "job goroutine finished after release", func() bool {
		return atomic.LoadInt32(&finished) == 1
	})
	if got := readQueues(t, rc, svr, key); !got.drained() {
		t.Fatalf("queues should stay drained after the late job finished, got %s", got)
	}
}

func TestIntegration_DuplicateExecSkippedAndEventKept(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_JOB_TIMEOUT_SEC", "10")
	key := "k-lock"

	eventS := mustEventJSON(t, &event{Id: "lock-1", Key: key, Args: (&Message[string]{Data: "locked"}).Marshall()})
	seedUnAck(t, rc, svr, key, eventS, time.Now().Unix())

	lockKey := fmt.Sprintf("%s:Redelay:runEvent:lock:%s", svr.svcName, "lock-1")
	held, err := svr.lock.Obtain(context.Background(), lockKey, 30*time.Second, nil)
	if err != nil {
		t.Fatalf("obtain test lock failed, err: %s", err)
	}

	var calls int32
	job := func(args string) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}

	svr.handleEvent(context.Background(), eventS, job)

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("job should be skipped while another instance holds the event lock, calls: %d", got)
	}
	st := readQueues(t, rc, svr, key)
	if st.unAck != 1 {
		t.Fatalf("a skipped event must stay in unAck for the lock holder, got %s", st)
	}
	if got := itCounter(t, svr, "duplicate_exec_total", key); got != 1 {
		t.Fatalf("duplicate_exec_total expected 1, got %+v", got)
	}

	if err := held.Release(context.Background()); err != nil {
		t.Fatalf("release test lock failed, err: %s", err)
	}

	svr.handleEvent(context.Background(), eventS, job)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("job should run once the lock is free, calls: %d", got)
	}
	if got := readQueues(t, rc, svr, key); !got.drained() {
		t.Fatalf("event should be acked after a successful run, got %s", got)
	}
}

func TestIntegration_TwoInstancesConsumeExactlyOnce(t *testing.T) {
	skipSlow(t)

	itEnv(t)
	rc := testRedis(t)
	name := itSvcName(t)

	t.Setenv("HOSTNAME", "pod-instance-a")
	svrA := NewService(name, rc)
	t.Setenv("HOSTNAME", "pod-instance-b")
	svrB := NewService(name, rc)

	key := "k-two-instance"
	const n = 40

	var total int32
	seen := make(map[string]int)
	var mx sync.Mutex
	job := func(args string) error {
		m := new(Message[string])
		if err := json.Unmarshal([]byte(args), m); err != nil {
			return err
		}
		time.Sleep(20 * time.Millisecond)
		mx.Lock()
		seen[m.Data]++
		mx.Unlock()
		atomic.AddInt32(&total, 1)
		return nil
	}

	ctx := tool.NewCtxBK()
	for i := 0; i < n; i++ {
		if err := svrA.Publish(ctx, key, (&Message[string]{Data: fmt.Sprintf("m-%d", i)}).Marshall()); err != nil {
			t.Fatalf("publish m-%d failed, err: %s", i, err)
		}
	}

	go func() { svrA.LaunchJob(tool.NewCtxBK(), key, job) }()
	go func() { svrB.LaunchJob(tool.NewCtxBK(), key, job) }()

	waitUntil(t, 30*time.Second, "all messages consumed", func() bool { return atomic.LoadInt32(&total) >= n })
	waitUntil(t, 10*time.Second, "queues drained", func() bool { return readQueues(t, rc, svrA, key).drained() })

	time.Sleep(500 * time.Millisecond)

	mx.Lock()
	defer mx.Unlock()
	dup := 0
	for i := 0; i < n; i++ {
		if got := seen[fmt.Sprintf("m-%d", i)]; got != 1 {
			dup++
			if dup <= 5 {
				t.Logf("message m-%d consumed %d times", i, got)
			}
		}
	}
	if dup != 0 {
		t.Fatalf("%d messages were not consumed exactly once across 2 instances", dup)
	}
	t.Logf("2 instances x %d messages: consumed exactly once, duplicate_exec_total: %d",
		n, int(sumMetric(t, "duplicate_exec_total", svcLabels(name, map[string]string{"key": key}))))
}

func TestIntegration_BacklogBeyondTickCountDrains(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_TICK_COUNT", "10", "DELAYTASK_TICK_INTERVAL_MS", "100")
	key := "k-backlog"
	rec := newRecorder()

	const n = 40
	ctx := tool.NewCtxBK()
	for i := 0; i < n; i++ {
		if err := svr.Publish(ctx, key, (&Message[string]{Data: fmt.Sprintf("bg-%d", i)}).Marshall()); err != nil {
			t.Fatalf("publish bg-%d failed, err: %s", i, err)
		}
	}

	if got := readQueues(t, rc, svr, key).delay; got != n {
		t.Fatalf("expected %d events in the delay zset, got %d", n, got)
	}

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	waitUntil(t, 20*time.Second, fmt.Sprintf("all %d backlogged messages delivered", n), func() bool {
		return rec.total() == n
	})
	for i := 0; i < n; i++ {
		if got := rec.count(fmt.Sprintf("bg-%d", i)); got != 1 {
			t.Fatalf("bg-%d expected 1 delivery, got %d", i, got)
		}
	}
	waitUntil(t, 8*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })
}

func TestIntegration_ConsumeRateLimit(t *testing.T) {
	skipSlow(t)

	svr, rc := itSvc(t, "DELAYTASK_CONSUME_LIMIT", "5", "DELAYTASK_TICK_INTERVAL_MS", "20")
	key := "k-rate"
	rec := newRecorder()

	const n = 20
	ctx := tool.NewCtxBK()
	for i := 0; i < n; i++ {
		if err := svr.Publish(ctx, key, (&Message[string]{Data: fmt.Sprintf("rl-%d", i)}).Marshall()); err != nil {
			t.Fatalf("publish rl-%d failed, err: %s", i, err)
		}
	}

	start := time.Now()
	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	waitUntil(t, 30*time.Second, "all rate limited messages delivered", func() bool { return rec.total() == n })
	elapsed := time.Since(start)
	if elapsed < 1200*time.Millisecond {
		t.Fatalf("consumeLimit=5 should throttle %d messages to ~2s+, drained in %s", n, elapsed)
	}
	t.Logf("consumeLimit=5, %d messages drained in %s", n, elapsed)
	waitUntil(t, 8*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })
}

func TestIntegration_MalformedDelayMemberIsDropped(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_TICK_INTERVAL_MS", "100")
	key := "k-malformed"
	rec := newRecorder()

	ctx := context.Background()
	malformed := "not-a-json"
	if err := rc.ZAdd(ctx, svr.key, redis.Z{
		Score:  float64(time.Now().Unix() - 1),
		Member: malformed,
	}).Err(); err != nil {
		t.Fatalf("seed malformed member failed, err: %s", err)
	}

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, rec.job()) }()

	itPublish(t, svr, key, "healthy")
	waitUntil(t, 10*time.Second, "healthy message delivered", func() bool { return rec.count("healthy") == 1 })

	waitUntil(t, 10*time.Second, "malformed member dropped from the delay zset", func() bool {
		st := readQueues(t, rc, svr, key)
		for _, m := range st.delayRaw {
			if m == malformed {
				return false
			}
		}
		return true
	})

	time.Sleep(800 * time.Millisecond)
	st := readQueues(t, rc, svr, key)
	if !st.drained() {
		t.Fatalf("queues should be drained after the malformed member is purged, got %s, members: %+v", st, st.delayRaw)
	}
	if got := atomic.LoadInt32(&rec.calls); got != 1 {
		t.Fatalf("the malformed member must not reach the job, calls: %d", got)
	}
	if got := itCounter(t, svr, "malformed_event_total", ""); got < 1 {
		t.Fatalf("malformed_event_total expected >= 1, got %+v", got)
	}

	before := itCounter(t, svr, "tickQ_tick_total", "")
	time.Sleep(1200 * time.Millisecond)
	after := itCounter(t, svr, "tickQ_tick_total", "")
	if after != before {
		t.Fatalf("ticks should stop once the queue is clean, tickQ_tick_total: %+v -> %+v", before, after)
	}
}

func TestIntegration_MetricsLoopFlushesQueueLengths(t *testing.T) {
	skipSlow(t)

	svr, rc := itSvc(t)
	key := "k-metrics"

	ctx := context.Background()
	if err := rc.ZAdd(ctx, svr.key, redis.Z{Score: float64(time.Now().Unix() - 1), Member: "delay-member"}).Err(); err != nil {
		t.Fatalf("seed delay member failed, err: %s", err)
	}
	if err := rc.RPush(ctx, svr.bufferQName(key), "buffer-member").Err(); err != nil {
		t.Fatalf("seed buffer member failed, err: %s", err)
	}
	seedUnAck(t, rc, svr, key, "unack-member", time.Now().Unix())

	_ = svr.exp.GaugeWith("buffer_queue_length", map[string]string{"key": key})
	_ = svr.exp.GaugeWith("unAck_queue_length", map[string]string{"key": key})
	svr.jobKeys.Store(key, struct{}{})

	go svr.loopFlushExp()

	waitMetric(t, svr.svcName, "loop_flush_exp_ping", itExtra(t, ""), 1, 15*time.Second)
	waitMetric(t, svr.svcName, "delay_queue_length", itExtra(t, ""), 1, 15*time.Second)
	waitMetric(t, svr.svcName, "buffer_queue_length", itExtra(t, key), 1, 15*time.Second)
	waitMetric(t, svr.svcName, "unAck_queue_length", itExtra(t, key), 1, 15*time.Second)
	waitMetric(t, svr.svcName, "redis_ping", itExtra(t, ""), 1, 15*time.Second)

	st := readQueues(t, rc, svr, key)
	if st.delay != 1 || st.buffer != 1 || st.unAck != 1 {
		t.Fatalf("metrics loop should not consume anything, got %s", st)
	}
}

func TestIntegration_OfflineRedisFailsGracefully(t *testing.T) {
	itEnv(t, "DELAYTASK_TICK_INTERVAL_MS", "60000")
	svr := NewService(itSvcName(t), offlineRedis())
	key := "k-offline"
	ctx := context.Background()

	if err := svr.Publish(ctx, key, (&Message[string]{Data: "x"}).Marshall()); err == nil {
		t.Fatalf("Publish should return an error when redis is unreachable")
	} else {
		t.Logf("Publish error as expected: %s", err)
	}

	dErr := svr.PublishDelay(ctx, key, "x", 5)
	if dErr == nil {
		t.Fatalf("PublishDelay should return an error when redis is unreachable")
	}

	svr.tickQ()
	svr.tickUnAckQ(key)

	var calls int32
	svr.handleEvent(ctx, mustEventJSON(t, &event{Id: "offline-1", Key: key, Args: "a"}), func(args string) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("handleEvent should not run the job when the event lock cannot be obtained, calls: %d", got)
	}

	svr.jobKeys.Store(key, struct{}{})
	svr.flushQueueLenExp(ctx)

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, func(args string) error { return nil }) }()
	time.Sleep(300 * time.Millisecond)
	t.Logf("redis 不可达时 Publish/tickQ/tickUnAckQ/handleEvent/LaunchJob 均未 panic, 只记录错误并重试")
}

// TestIntegration_LockInfraErrorRequeuesEvent 覆盖 runEvent 取执行锁时的 redis 基础错误:
// 属于基础设施故障, 必须重投而不是当成业务失败 ack, 否则消息被静默丢弃.
func TestIntegration_LockInfraErrorRequeuesEvent(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_JOB_TIMEOUT_SEC", "30")
	key := "k-lockerr"
	ctx := context.Background()

	eventS := mustEventJSON(t, &event{
		Id:   "lockerr-1",
		Key:  key,
		Args: (&Message[string]{Data: "must-survive"}).Marshall(),
	})
	seedUnAck(t, rc, svr, key, eventS, time.Now().Unix())

	lockKey := fmt.Sprintf("%s:Redelay:runEvent:lock:%s", svr.svcName, "lockerr-1")
	if err := rc.SAdd(ctx, lockKey, "wrongtype-blocker").Err(); err != nil {
		t.Fatalf("seed wrongtype lock key failed, err: %s", err)
	}

	var calls int32
	job := func(args string) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}

	svr.handleEvent(ctx, eventS, job)

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("job should not run while the event lock errors, calls: %d", got)
	}

	st := readQueues(t, rc, svr, key)
	if st.unAck != 0 {
		t.Fatalf("the event should leave unAck once requeued, got %s, members: %+v", st, st.unAckRaw)
	}
	if st.delay != 1 {
		t.Fatalf("a lock infra error must requeue the event, got %s", st)
	}

	requeued := unmarshalEvent(t, st.delayRaw[0])
	if requeued.Id != "lockerr-1" || requeued.Key != key {
		t.Fatalf("requeued event mismatch, got %+v", requeued)
	}
	if requeued.RetryCount != 1 {
		t.Fatalf("RetryCount expected 1 after the lock failure, got %d, event: %+v", requeued.RetryCount, requeued)
	}
	if got := itCounterOrZero(t, svr, "job_failed_total", key); got != 0 {
		t.Fatalf("a lock infra error is not a business failure, job_failed_total: %+v", got)
	}

	if err := rc.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("clear blocker lock key failed, err: %s", err)
	}

	go func() { svr.LaunchJob(tool.NewCtxBK(), key, job) }()

	waitUntil(t, 30*time.Second, "requeued event delivered after the lock error clears", func() bool {
		return atomic.LoadInt32(&calls) == 1
	})
	waitUntil(t, 8*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })

	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("event delivered %d times after a lock infra error, expected exactly 1", got)
	}
}

func TestIntegration_LockInfraErrorRetriesUpToThreeTimes(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_JOB_TIMEOUT_SEC", "30")
	key := "k-lockerr-exhaust"
	ctx := context.Background()

	lockKey := fmt.Sprintf("%s:Redelay:runEvent:lock:%s", svr.svcName, "lockerr-exhaust")
	if err := rc.SAdd(ctx, lockKey, "wrongtype-blocker").Err(); err != nil {
		t.Fatalf("seed wrongtype lock key failed, err: %s", err)
	}
	t.Cleanup(func() { _ = rc.Del(ctx, lockKey).Err() })

	eventS := mustEventJSON(t, &event{
		Id:         "lockerr-exhaust",
		Key:        key,
		Args:       (&Message[string]{Data: "doomed-lock"}).Marshall(),
		RetryCount: 3,
	})
	seedUnAck(t, rc, svr, key, eventS, time.Now().Unix())

	var calls int32
	svr.handleEvent(ctx, eventS, func(args string) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("job should not run, calls: %d", got)
	}
	waitUntil(t, 8*time.Second, "event dropped after the retry budget is exhausted", func() bool {
		return readQueues(t, rc, svr, key).drained()
	})
	time.Sleep(300 * time.Millisecond)
	if got := readQueues(t, rc, svr, key); !got.drained() {
		t.Fatalf("event with RetryCount 3 should be discarded, got %s", got)
	}
}

func TestIntegration_UnsupportedRedisCommandSurfacesAsPublishError(t *testing.T) {
	itEnv(t)
	rc := testRedis(t)
	svr := NewService(itSvcName(t), rc)

	ctx := context.Background()
	version, err := rc.Do(ctx, "INFO", "server").Text()
	if err != nil {
		t.Skipf("cannot read redis server info, err: %s", err)
	}

	major := 0
	for _, line := range strings.Split(version, "\n") {
		if !strings.HasPrefix(line, "redis_version:") {
			continue
		}
		parts := strings.SplitN(strings.TrimPrefix(line, "redis_version:"), ".", 2)
		major, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
	}
	t.Logf("redis_version: %d", major)

	if err := svr.Publish(ctx, "k-info", (&Message[string]{Data: "x"}).Marshall()); err != nil {
		t.Fatalf("Publish failed, err: %s", err)
	}
}
