package Redelay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/redis/go-redis/v9"
)

// peakTracker records the highest number of handlers running at the same moment.
type peakTracker struct {
	current int32
	peak    int32
	done    int32
}

func (p *peakTracker) enter(sleep time.Duration) func() {
	cur := atomic.AddInt32(&p.current, 1)
	for {
		old := atomic.LoadInt32(&p.peak)
		if cur <= old || atomic.CompareAndSwapInt32(&p.peak, old, cur) {
			break
		}
	}
	return func() {
		time.Sleep(sleep)
		atomic.AddInt32(&p.done, 1)
		atomic.AddInt32(&p.current, -1)
	}
}

func TestIntegration_ConcurrencyCapBoundsInFlightHandlers(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_CONCURRENCY", "2", "DELAYTASK_TICK_INTERVAL_MS", "20")
	key := "k-cap"

	if svr.concurrency != 2 {
		t.Fatalf("concurrency expected 2, got %d", svr.concurrency)
	}
	if cap(svr.slots) != 2 {
		t.Fatalf("slot pool capacity expected 2, got %d", cap(svr.slots))
	}

	const n = 8
	ctx := tool.NewCtxBK()
	for i := 0; i < n; i++ {
		if err := svr.Publish(ctx, key, (&Message[string]{Data: payloadName("cap", i)}).Marshall()); err != nil {
			t.Fatalf("publish %d failed, err: %s", i, err)
		}
	}

	peak := &peakTracker{}
	go func() {
		svr.LaunchJob(ctx, key, func(args string) error {
			end := peak.enter(250 * time.Millisecond)
			defer end()
			return nil
		})
	}()

	waitUntil(t, 30*time.Second, "all handlers finished", func() bool {
		return atomic.LoadInt32(&peak.done) == n
	})
	waitUntil(t, 8*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })

	got := atomic.LoadInt32(&peak.peak)
	if got > 2 {
		t.Fatalf("in-flight handlers %d exceeded the concurrency cap 2", got)
	}
	if got != 2 {
		t.Fatalf("with %d queued events and a 250ms handler the cap should be reached, peak was %d", n, got)
	}
	if got := len(svr.slots); got != 0 {
		t.Fatalf("slot pool should be fully returned after drain, %d slots still held", got)
	}
	t.Logf("cap=2, %d events: peak in-flight %d, slot pool back to %d", n, peak.peak, len(svr.slots))
}

func TestIntegration_ConcurrencyCapIsSharedAcrossKeys(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_CONCURRENCY", "2", "DELAYTASK_TICK_INTERVAL_MS", "20")
	keyA, keyB := "k-cap-a", "k-cap-b"

	ctx := tool.NewCtxBK()
	const perKey = 4
	for i := 0; i < perKey; i++ {
		if err := svr.Publish(ctx, keyA, (&Message[string]{Data: payloadName("capa", i)}).Marshall()); err != nil {
			t.Fatalf("publish keyA %d failed, err: %s", i, err)
		}
		if err := svr.Publish(ctx, keyB, (&Message[string]{Data: payloadName("capb", i)}).Marshall()); err != nil {
			t.Fatalf("publish keyB %d failed, err: %s", i, err)
		}
	}

	shared := &peakTracker{}
	job := func(args string) error {
		end := shared.enter(300 * time.Millisecond)
		defer end()
		return nil
	}

	go func() { svr.LaunchJob(ctx, keyA, job) }()
	go func() { svr.LaunchJob(ctx, keyB, job) }()

	waitUntil(t, 30*time.Second, "all handlers across both keys finished", func() bool {
		return atomic.LoadInt32(&shared.done) == perKey*2
	})
	waitUntil(t, 8*time.Second, "keyA drained", func() bool { return readQueues(t, rc, svr, keyA).drained() })
	waitUntil(t, 8*time.Second, "keyB drained", func() bool { return readQueues(t, rc, svr, keyB).drained() })

	if got := atomic.LoadInt32(&shared.peak); got > 2 {
		t.Fatalf("the cap must be per Svc, but %d handlers ran at once across two keys", got)
	}
	if got := len(svr.slots); got != 0 {
		t.Fatalf("slot pool should be fully returned, %d slots still held", got)
	}
	t.Logf("cap=2 shared by 2 keys: peak in-flight across both = %d", shared.peak)
}

func TestIntegration_SlotHeldOnlyWhileHandlerRuns(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_CONCURRENCY", "2")
	key := "k-slot-life"

	itPublish(t, svr, key, "slot-life")

	var started, finished int32
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(tool.NewCtxBK())
	t.Cleanup(cancel)

	go func() {
		svr.LaunchJob(ctx, key, func(args string) error {
			atomic.AddInt32(&started, 1)
			<-release
			atomic.AddInt32(&finished, 1)
			return nil
		})
	}()

	waitUntil(t, 10*time.Second, "handler claimed the event", func() bool {
		st := readQueues(t, rc, svr, key)
		return atomic.LoadInt32(&started) == 1 && st.unAck == 1
	})
	if got := len(svr.slots); got != 1 {
		t.Fatalf("exactly one slot should be held while the handler runs, got %d", got)
	}

	close(release)

	waitUntil(t, 10*time.Second, "queues drained", func() bool { return readQueues(t, rc, svr, key).drained() })
	waitUntil(t, 5*time.Second, "slot returned after the handler finished", func() bool {
		return len(svr.slots) == 0
	})
	if got := atomic.LoadInt32(&finished); got != 1 {
		t.Fatalf("handler runs expected 1, got %d", got)
	}
}

func TestIntegration_ClaimEventReturnsSlotOnEmptyQueue(t *testing.T) {
	svr, _ := itSvc(t, "DELAYTASK_CONCURRENCY", "3")
	ctx := tool.NewCtxBK()

	for i := 0; i < 20; i++ {
		eventS, err := svr.claimEvent(ctx, "k-empty")
		if !errors.Is(err, redis.Nil) {
			t.Fatalf("claim on an empty queue expected redis.Nil, got eventS: %q, err: %+v", eventS, err)
		}
		if got := len(svr.slots); got != 0 {
			t.Fatalf("claim %d leaked a slot, %d slots held", i, got)
		}
	}

	if cap(svr.slots) != 3 {
		t.Fatalf("slot pool capacity expected 3, got %d", cap(svr.slots))
	}
}

func TestIntegration_ClaimEventHonoursCtxCancelWhileWaitingForSlot(t *testing.T) {
	svr, _ := itSvc(t, "DELAYTASK_CONCURRENCY", "1")

	svr.slots <- struct{}{}

	ctx, cancel := context.WithCancel(tool.NewCtxBK())
	returned := make(chan error, 1)
	go func() {
		_, err := svr.claimEvent(ctx, "k-cap-full")
		returned <- err
	}()

	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-returned:
		t.Fatalf("claimEvent should block while the pool is full, it returned err: %+v", err)
	default:
	}

	cancel()

	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("claimEvent should surface the cancellation, got %+v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("claimEvent did not return after ctx cancel, a full slot pool blocks shutdown")
	}

	if got := len(svr.slots); got != 1 {
		t.Fatalf("the cancelled claim should not consume a slot, held: %d", got)
	}
	<-svr.slots
}

func TestIntegration_ClaimEventClaimsHeadEvent(t *testing.T) {
	svr, rc := itSvc(t, "DELAYTASK_CONCURRENCY", "3")
	key := "k-claim"
	ctx := tool.NewCtxBK()

	headPayload := (&Message[string]{MsgId: "m-claim-0", Data: payloadName("claim", 0)}).Marshall()
	head := mustEventJSON(t, &event{Id: "id-claim-0", Key: key, Args: headPayload})
	second := mustEventJSON(t, &event{
		Id:   "id-claim-1",
		Key:  key,
		Args: (&Message[string]{MsgId: "m-claim-1", Data: payloadName("claim", 1)}).Marshall(),
	})

	if err := rc.RPush(ctx, svr.bufferQName(key), head, second).Err(); err != nil {
		t.Fatalf("seed buffer queue failed, err: %s", err)
	}

	eventS, err := svr.claimEvent(ctx, key)
	if err != nil {
		t.Fatalf("claim failed, err: %s", err)
	}
	if eventS != head {
		t.Fatalf("claim should take the queue head, got: %s", eventS)
	}

	claimed := unmarshalEvent(t, eventS)
	if claimed.Id != "id-claim-0" || claimed.Key != key {
		t.Fatalf("claimed event mismatch, got: %+v", claimed)
	}
	decoded := new(Message[string])
	if err := json.Unmarshal([]byte(claimed.Args), decoded); err != nil {
		t.Fatalf("unmarshal claimed args failed, args: %s, err: %s", claimed.Args, err)
	}
	if decoded.MsgId != "m-claim-0" || decoded.Data != payloadName("claim", 0) {
		t.Fatalf("claimed payload mismatch, got: %+v", decoded)
	}

	st := readQueues(t, rc, svr, key)
	if st.buffer != 1 || st.unAck != 1 {
		t.Fatalf("expected the head moved into unAck, got %s, buffer: %+v", st, st.bufferRaw)
	}
	if len(st.unAckRaw) != 1 || st.unAckRaw[0] != head {
		t.Fatalf("unAck should hold the claimed member, got: %+v", st.unAckRaw)
	}
	if got := len(svr.slots); got != 1 {
		t.Fatalf("a successful claim hands the slot to the caller, held: %d", got)
	}

	<-svr.slots
}

func payloadName(prefix string, i int) string {
	return fmt.Sprintf("%s-%d", prefix, i)
}
