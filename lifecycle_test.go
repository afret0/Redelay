package Redelay

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/afret0/wheel/tool"
)

// TestNewJob_TimeoutAlignsWithJobTimeout covers the NewJob ctx deadline: it comes from the
// very same env as Svc.jobTimeout, so the handler ctx expires together with the hard
// timeout gate instead of the old hardcoded 30s.
func TestNewJob_TimeoutAlignsWithJobTimeout(t *testing.T) {
	t.Setenv("HOSTNAME", "")
	t.Setenv("DELAYTASK_REDIS_CLUSTER", "")

	cases := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"default", "", 60 * time.Second},
		{"fromEnv", "5", 5 * time.Second},
		{"invalidEnvKeepsDefault", "abc", 60 * time.Second},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("DELAYTASK_JOB_TIMEOUT_SEC", c.env)

			svr := NewService(itSvcName(t), offlineRedis())
			if svr.jobTimeout != c.want {
				t.Fatalf("jobTimeout expected %s, got %s", c.want, svr.jobTimeout)
			}

			var deadline time.Time
			var hasDeadline bool
			job := NewJob(func(ctx context.Context, p string) error {
				deadline, hasDeadline = ctx.Deadline()
				if ctx.Value("opId") != "m-timeout" {
					t.Errorf("opId expected m-timeout, got %+v", ctx.Value("opId"))
				}
				return nil
			})

			start := time.Now()
			msg := (&Message[string]{MsgId: "m-timeout", Data: "payload"}).Marshall()
			if err := job(msg); err != nil {
				t.Fatalf("job failed, err: %s", err)
			}

			if !hasDeadline {
				t.Fatalf("handler ctx should carry the jobTimeout deadline")
			}

			got := deadline.Sub(start)
			if got > c.want+time.Second || got < c.want-2*time.Second {
				t.Fatalf("handler ctx deadline expected ~%s, got %s", c.want, got)
			}
			t.Logf("NewJob ctx deadline: %s (jobTimeout %s)", got, c.want)
		})
	}
}

// TestNewJob_TimeoutResolvedAtConstruction pins that the env is read once when the Job is
// built, not per message.
func TestNewJob_TimeoutResolvedAtConstruction(t *testing.T) {
	t.Setenv("HOSTNAME", "")
	t.Setenv("DELAYTASK_JOB_TIMEOUT_SEC", "8")

	var deadline time.Time
	job := NewJob(func(ctx context.Context, p string) error {
		deadline, _ = ctx.Deadline()
		return nil
	})

	t.Setenv("DELAYTASK_JOB_TIMEOUT_SEC", "800")

	start := time.Now()
	if err := job((&Message[string]{MsgId: "m-1", Data: "x"}).Marshall()); err != nil {
		t.Fatalf("job failed, err: %s", err)
	}

	got := deadline.Sub(start)
	if got > 9*time.Second || got < 6*time.Second {
		t.Fatalf("deadline should still be the 8s resolved at construction, got %s", got)
	}
}

// TestLaunchJob_StopsOnCtxCancel covers the cancellation path: LaunchJob returns, the
// consume_ping defer really runs, and returning early on limiter.Wait error keeps it from
// spinning in a log flooding busy loop. The limiter is set to 1/s on purpose:
// rate.Limiter.Wait only honours cancellation while it is actually waiting for a token, so
// a burst that never runs out would take the redis.Nil idle path instead and hide the bug.
func TestLaunchJob_StopsOnCtxCancel(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_CONSUME_LIMIT", "1", "DELAYTASK_TICK_INTERVAL_MS", "10")
	key := "k-cancel"

	var calls int32
	ctx, cancel := context.WithCancel(tool.NewCtxBK())
	returned := make(chan struct{})

	go func() {
		svr.LaunchJob(ctx, key, func(args string) error {
			atomic.AddInt32(&calls, 1)
			return nil
		})
		close(returned)
	}()

	waitUntil(t, 8*time.Second, "consume_ping up", func() bool {
		return itGaugeValue(t, svr, "consume_ping", map[string]string{"key": key}) == 1
	})

	// burst = consumeLimit * 2, so after this the loop is blocked inside limiter.Wait.
	time.Sleep(400 * time.Millisecond)

	cancel()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatalf("LaunchJob did not return after ctx cancel, limiter.Wait error path is spinning")
	}

	waitUntil(t, 8*time.Second, "consume_ping back to 0", func() bool {
		return itGaugeValue(t, svr, "consume_ping", map[string]string{"key": key}) == 0
	})

	itPublish(t, svr, key, "after-cancel")
	svr.tickQ()

	time.Sleep(500 * time.Millisecond)
	st := readQueues(t, rc, svr, key)
	if st.buffer != 1 || st.unAck != 0 {
		t.Fatalf("nobody should claim events after cancel, got %s, buffer: %+v", st, st.bufferRaw)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("handler must not run after cancel, calls: %d", got)
	}
}

// TestLaunchJob_DrainWaitsForInFlightAndAcks covers the graceful shutdown semantics:
// cancelling stops claiming, in-flight handlers run to completion with a detached ctx,
// their ack lands, so those events are not requeued and never executed twice.
func TestLaunchJob_DrainWaitsForInFlightAndAcks(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_JOB_TIMEOUT_SEC", "30")
	key := "k-drain"

	itPublish(t, svr, key, "drain-me")

	var mx sync.Mutex
	var handlerCtxErr error
	var started, finished int32
	release := make(chan struct{})

	job := NewJob(func(ctx context.Context, p string) error {
		atomic.AddInt32(&started, 1)
		<-release

		mx.Lock()
		handlerCtxErr = ctx.Err()
		mx.Unlock()

		atomic.AddInt32(&finished, 1)
		return nil
	})

	ctx, cancel := context.WithCancel(tool.NewCtxBK())
	returned := make(chan struct{})
	go func() {
		svr.LaunchJob(ctx, key, job)
		close(returned)
	}()

	waitUntil(t, 10*time.Second, "event claimed and handler running", func() bool {
		st := readQueues(t, rc, svr, key)
		return atomic.LoadInt32(&started) == 1 && st.unAck == 1 && st.delay == 0 && st.buffer == 0
	})

	cancel()

	select {
	case <-returned:
		t.Fatalf("LaunchJob returned while a handler was still in flight, drain did not wait")
	case <-time.After(800 * time.Millisecond):
		t.Logf("drain: 取消后 LaunchJob 仍在等在飞任务, unAck 未被重投")
	}

	close(release)

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatalf("LaunchJob did not return after the in-flight handler finished")
	}

	mx.Lock()
	defer mx.Unlock()

	if handlerCtxErr != nil {
		t.Fatalf("the in-flight handler ctx must be detached from the cancelled consumer ctx, got: %s", handlerCtxErr)
	}
	if got := atomic.LoadInt32(&started); got != 1 {
		t.Fatalf("handler should run exactly once, got %d", got)
	}
	if got := atomic.LoadInt32(&finished); got != 1 {
		t.Fatalf("handler should finish exactly once, got %d", got)
	}

	waitUntil(t, 8*time.Second, "the in-flight event was acked, so it is not requeued", func() bool {
		return readQueues(t, rc, svr, key).drained()
	})

	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt32(&started); got != 1 {
		t.Fatalf("the drained event was executed again, started: %d", got)
	}
	if got := readQueues(t, rc, svr, key); !got.drained() {
		t.Fatalf("queues should stay drained, got %s", got)
	}
}

// TestLaunchJob_DrainBoundedByJobTimeout documents that waiting for in-flight handlers is
// bounded by the jobTimeout gate, so a hung handler cannot block shutdown forever.
func TestLaunchJob_DrainBoundedByJobTimeout(t *testing.T) {
	skipSlow(t)

	svr, rc := itSvc(t, "DELAYTASK_JOB_TIMEOUT_SEC", "2")
	key := "k-drain-timeout"

	itPublish(t, svr, key, "hangs-forever")

	release := make(chan struct{})
	var started int32
	job := func(args string) error {
		atomic.AddInt32(&started, 1)
		<-release
		return nil
	}
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(tool.NewCtxBK())
	returned := make(chan struct{})
	go func() {
		svr.LaunchJob(ctx, key, job)
		close(returned)
	}()

	waitUntil(t, 10*time.Second, "hung handler started", func() bool { return atomic.LoadInt32(&started) == 1 })

	cancel()
	drainStart := time.Now()

	select {
	case <-returned:
	case <-time.After(15 * time.Second):
		t.Fatalf("drain is not bounded: LaunchJob still blocked on a hung handler")
	}

	elapsed := time.Since(drainStart)
	if elapsed > 8*time.Second {
		t.Fatalf("drain should end with the jobTimeout gate (%s), took %s", svr.jobTimeout, elapsed)
	}
	t.Logf("drain 被 jobTimeout(2s) 硬闸门兜住, 实际耗时 %s", elapsed)

	waitUntil(t, 8*time.Second, "timeout gate acked the hung event", func() bool {
		return readQueues(t, rc, svr, key).drained()
	})
}
