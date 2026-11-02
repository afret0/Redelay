package Redelay

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestGetLogger_Singleton(t *testing.T) {
	t.Setenv("HOSTNAME", "")

	first := GetLogger()
	second := GetLogger()
	if first != second {
		t.Fatalf("GetLogger returned different logger instances")
	}
	if first == nil {
		t.Fatalf("GetLogger returned nil")
	}
	if first.Level != logrus.InfoLevel {
		t.Fatalf("logger level expected info, got %s", first.Level)
	}
	if _, isJSON := first.Formatter.(*logrus.JSONFormatter); !isJSON {
		t.Fatalf("logger formatter expected JSONFormatter, got %T", first.Formatter)
	}
	if first.Out != os.Stderr {
		t.Fatalf("logger should write to stderr by default, got %T", first.Out)
	}
}

func TestCtxLogger_OpId(t *testing.T) {
	t.Setenv("HOSTNAME", "")

	withOpId := CtxLogger(newCtxWithOpId("op-abc"))
	if got := withOpId.Data["opId"]; got != "op-abc" {
		t.Fatalf("opId expected op-abc, got: %+v", got)
	}

	withoutOpId := CtxLogger(newCtxWithOpId(""))
	if _, ok := withoutOpId.Data["opId"]; !ok {
		t.Fatalf("opId field should always exist, got: %+v", withoutOpId.Data)
	}
	if got := withoutOpId.Data["opId"]; got != "" {
		t.Fatalf("opId expected empty string, got: %+v", got)
	}
}

func TestNewService_PanicOnEmptyName(t *testing.T) {
	t.Setenv("HOSTNAME", "")

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("NewService(\"\") should panic")
		}
		if r != "svcName is required" {
			t.Fatalf("unexpected panic value: %+v", r)
		}
	}()

	_ = NewService("", offlineRedis())
	t.Fatalf("NewService(\"\") did not panic")
}

func TestNewService_Defaults(t *testing.T) {
	t.Setenv("HOSTNAME", "")
	t.Setenv("DELAYTASK_TICK_COUNT", "")
	t.Setenv("DELAYTASK_TICK_INTERVAL_MS", "")
	t.Setenv("DELAYTASK_CONSUME_LIMIT", "")
	t.Setenv("DELAYTASK_UNACK_TIMEOUT_SEC", "")
	t.Setenv("DELAYTASK_JOB_TIMEOUT_SEC", "")
	t.Setenv("DELAYTASK_REDIS_CLUSTER", "")

	s := NewService(itSvcName(t)+"-defaults", offlineRedis())

	if s.svcName != itSvcName(t)+"-defaults" {
		t.Fatalf("svcName mismatch: %s", s.svcName)
	}
	if s.key != s.svcName+":Redelay" {
		t.Fatalf("delay queue key mismatch: %s", s.key)
	}
	if s.bufferQName("kk") != s.svcName+":Redelay:bufferQueue:kk" {
		t.Fatalf("bufferQ name mismatch: %s", s.bufferQName("kk"))
	}
	if s.unAckQName("kk") != s.svcName+":Redelay:unAck:kk" {
		t.Fatalf("unAckQ name mismatch: %s", s.unAckQName("kk"))
	}
	if s.tickQCount != 500 {
		t.Fatalf("tickQCount expected 500, got: %d", s.tickQCount)
	}
	if s.tickInterval != 500 {
		t.Fatalf("tickInterval expected 500, got: %d", s.tickInterval)
	}
	if s.consumeLimit != 2500 {
		t.Fatalf("consumeLimit expected 2500(count*5), got: %d", s.consumeLimit)
	}
	if s.unAckTimeout != 3*time.Minute {
		t.Fatalf("unAckTimeout expected 3m, got: %s", s.unAckTimeout)
	}
	if s.jobTimeout != 60*time.Second {
		t.Fatalf("jobTimeout expected 60s, got: %s", s.jobTimeout)
	}
	if s.lock == nil {
		t.Fatalf("redislock client not initialized")
	}
	if s.exp == nil {
		t.Fatalf("exporter not initialized")
	}

	if got := itMetric(t, s.svcName, "tick_count", nil); got != 500 {
		t.Fatalf("gauge tick_count expected 500, got: %+v", got)
	}
	if got := itMetric(t, s.svcName, "tick_interval_ms", nil); got != 500 {
		t.Fatalf("gauge tick_interval_ms expected 500, got: %+v", got)
	}
	if got := itMetric(t, s.svcName, "consume_limit", nil); got != 2500 {
		t.Fatalf("gauge consume_limit expected 2500, got: %+v", got)
	}
}

func TestNewService_EnvOverrides(t *testing.T) {
	t.Setenv("HOSTNAME", "")
	t.Setenv("DELAYTASK_TICK_COUNT", "120")
	t.Setenv("DELAYTASK_TICK_INTERVAL_MS", "250")
	t.Setenv("DELAYTASK_CONSUME_LIMIT", "900")
	t.Setenv("DELAYTASK_JOB_TIMEOUT_SEC", "20")
	t.Setenv("DELAYTASK_UNACK_TIMEOUT_SEC", "100")

	s := NewService(itSvcName(t)+"-env", offlineRedis())

	if s.tickQCount != 120 {
		t.Fatalf("tickQCount expected 120, got: %d", s.tickQCount)
	}
	if s.tickInterval != 250 {
		t.Fatalf("tickInterval expected 250, got: %d", s.tickInterval)
	}
	if s.consumeLimit != 900 {
		t.Fatalf("consumeLimit expected 900, got: %d", s.consumeLimit)
	}
	if s.jobTimeout != 20*time.Second {
		t.Fatalf("jobTimeout expected 20s, got: %s", s.jobTimeout)
	}
	if s.unAckTimeout != 100*time.Second {
		t.Fatalf("unAckTimeout expected 100s, got: %s", s.unAckTimeout)
	}
}

func TestNewService_InvalidEnvKeepsDefault(t *testing.T) {
	t.Setenv("HOSTNAME", "")
	t.Setenv("DELAYTASK_TICK_COUNT", "not-a-number")
	t.Setenv("DELAYTASK_TICK_INTERVAL_MS", "")
	t.Setenv("DELAYTASK_CONSUME_LIMIT", "abc")
	t.Setenv("DELAYTASK_JOB_TIMEOUT_SEC", "xx")
	t.Setenv("DELAYTASK_UNACK_TIMEOUT_SEC", "yy")
	t.Setenv("DELAYTASK_REDIS_CLUSTER", "")

	s := NewService(itSvcName(t)+"-badenv", offlineRedis())

	if s.tickQCount != 500 || s.tickInterval != 500 || s.consumeLimit != 2500 {
		t.Fatalf("defaults not kept, got count: %d, interval: %d, limit: %d", s.tickQCount, s.tickInterval, s.consumeLimit)
	}
	if s.jobTimeout != 60*time.Second || s.unAckTimeout != 3*time.Minute {
		t.Fatalf("timeout defaults not kept, jobTimeout: %s, unAckTimeout: %s", s.jobTimeout, s.unAckTimeout)
	}
}

func TestNewService_UnAckTimeoutMustExceedJobTimeout(t *testing.T) {
	t.Setenv("HOSTNAME", "")

	cases := []struct {
		name      string
		unAckSec  string
		jobSec    string
		wantUnAck time.Duration
		wantJob   time.Duration
	}{
		{"equal", "60", "60", 120 * time.Second, 60 * time.Second},
		{"unAckSmaller", "10", "30", 60 * time.Second, 30 * time.Second},
		{"unAckLarger", "300", "30", 300 * time.Second, 30 * time.Second},
		{"defaultUnAckSmallerThanJob", "", "600", 1200 * time.Second, 600 * time.Second},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("DELAYTASK_UNACK_TIMEOUT_SEC", c.unAckSec)
			t.Setenv("DELAYTASK_JOB_TIMEOUT_SEC", c.jobSec)
			t.Setenv("DELAYTASK_REDIS_CLUSTER", "")

			s := NewService(itSvcName(t)+"-"+c.name, offlineRedis())
			if s.jobTimeout != c.wantJob {
				t.Fatalf("jobTimeout expected %s, got %s", c.wantJob, s.jobTimeout)
			}
			if s.unAckTimeout != c.wantUnAck {
				t.Fatalf("unAckTimeout expected %s, got %s", c.wantUnAck, s.unAckTimeout)
			}
			if s.unAckTimeout <= s.jobTimeout {
				t.Fatalf("unAckTimeout(%s) must be greater than jobTimeout(%s)", s.unAckTimeout, s.jobTimeout)
			}
		})
	}
}

func TestNewService_ClusterHashTag(t *testing.T) {
	t.Setenv("HOSTNAME", "")
	t.Setenv("DELAYTASK_REDIS_CLUSTER", "true")

	s := NewService(itSvcName(t)+"-cluster", offlineRedis())

	want := fmt.Sprintf("{%s:Redelay}", s.svcName)
	if s.key != want {
		t.Fatalf("cluster delay key expected %s, got %s", want, s.key)
	}
	if got, wantBuffer := s.bufferQName("k"), want+":bufferQueue:k"; got != wantBuffer {
		t.Fatalf("cluster bufferQ expected %s, got %s", wantBuffer, got)
	}
	if got, wantUnAck := s.unAckQName("k"), want+":unAck:k"; got != wantUnAck {
		t.Fatalf("cluster unAckQ expected %s, got %s", wantUnAck, got)
	}

	for _, key := range []string{s.key, s.bufferQName("k"), s.unAckQName("k")} {
		if !strings.HasPrefix(key, "{") || !strings.Contains(key, "}") {
			t.Fatalf("key %s lost cluster hash tag", key)
		}
		if tag := key[1:strings.Index(key, "}")]; tag != s.svcName+":Redelay" {
			t.Fatalf("key %s hash tag mismatch: %s", key, tag)
		}
	}
}

func TestNewService_ClusterDisabledByValue(t *testing.T) {
	t.Setenv("HOSTNAME", "")
	t.Setenv("DELAYTASK_REDIS_CLUSTER", "false")

	s := NewService(itSvcName(t)+"-nocluster", offlineRedis())
	if s.key != s.svcName+":Redelay" {
		t.Fatalf("cluster should stay disabled, key: %s", s.key)
	}
}

func TestDebug_Switch(t *testing.T) {
	s := NewService(itSvcName(t)+"-debug", offlineRedis())

	t.Setenv("REDELAY_DEBUG", "")
	if s.Debug() {
		t.Fatalf("Debug should be false when REDELAY_DEBUG unset")
	}
	t.Setenv("REDELAY_DEBUG", "on")
	if s.Debug() {
		t.Fatalf("Debug should be false for value 'on'")
	}
	for _, v := range []string{"true", "TRUE", "1", "yes", "YES"} {
		t.Setenv("REDELAY_DEBUG", v)
		if !s.Debug() {
			t.Fatalf("Debug should be true for value %q", v)
		}
	}
}

func TestTryRun_OncePerName(t *testing.T) {
	s := NewService(itSvcName(t)+"-tryrun", offlineRedis())

	release, ok := s.tryRun("job")
	if !ok || release == nil {
		t.Fatalf("first tryRun should succeed, ok: %t, release nil: %t", ok, release == nil)
	}

	if _, ok := s.tryRun("job"); ok {
		t.Fatalf("second tryRun with same name should fail")
	}
	if _, ok := s.tryRun("other"); !ok {
		t.Fatalf("tryRun with another name should succeed")
	}

	release()
	release2, ok := s.tryRun("job")
	if !ok || release2 == nil {
		t.Fatalf("tryRun after release should succeed, ok: %t", ok)
	}
	release2()
}

func TestTryRun_SecondReleaseIsNil(t *testing.T) {
	s := NewService(itSvcName(t)+"-tryrunnil", offlineRedis())

	release, ok := s.tryRun("job")
	if !ok {
		t.Fatalf("first tryRun should succeed")
	}

	release2, ok2 := s.tryRun("job")
	if ok2 != false || release2 != nil {
		t.Fatalf("occupied tryRun should return (nil,false), got ok: %t, release nil: %t", ok2, release2 == nil)
	}
	release()
}

// TestStartLoopGuards_SkipDuplicateLoop 验证 startTick / startTickUnAckQ 的单例守卫
// 与 REDELAY_DEBUG 无关: 同进程内重复的后台循环直接返回, 不碰 ping 打点.
func TestStartLoopGuards_SkipDuplicateLoop(t *testing.T) {
	t.Setenv("HOSTNAME", "")
	t.Setenv("DELAYTASK_TICK_INTERVAL_MS", "100")
	t.Setenv("DELAYTASK_REDIS_CLUSTER", "")

	for _, debug := range []string{"true", ""} {
		t.Run("debug_"+debugValueLabel(debug), func(t *testing.T) {
			t.Setenv("REDELAY_DEBUG", debug)
			s := NewService(itSvcName(t), offlineRedis())

			s.exp.Gauge("tick_ping").Set(0)
			unAckLabels := map[string]string{"key": "guard-key"}
			s.exp.GaugeWith("tick_unack_ping", unAckLabels).Set(0)

			tickRelease, ok := s.tryRun("tickQ")
			if !ok {
				t.Fatalf("tickQ tag should be free")
			}
			defer tickRelease()
			unAckRelease, ok := s.tryRun("tickUnAckQ:guard-key")
			if !ok {
				t.Fatalf("tickUnAckQ tag should be free")
			}
			defer unAckRelease()

			tickDone := make(chan struct{})
			unAckDone := make(chan struct{})
			go func() { s.startTick(); close(tickDone) }()
			go func() { s.startTickUnAckQ("guard-key"); close(unAckDone) }()

			select {
			case <-tickDone:
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("duplicate startTick should return immediately (REDELAY_DEBUG=%q)", debug)
			}
			select {
			case <-unAckDone:
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("duplicate startTickUnAckQ should return immediately (REDELAY_DEBUG=%q)", debug)
			}

			if got := itMetric(t, s.svcName, "tick_ping", nil); got != 0 {
				t.Fatalf("skipped startTick should not touch tick_ping, got %+v", got)
			}
			if got := itMetric(t, s.svcName, "tick_unack_ping", unAckLabels); got != 0 {
				t.Fatalf("skipped startTickUnAckQ should not touch tick_unack_ping, got %+v", got)
			}

			if _, live := s.tryRun("tickQ"); live {
				t.Fatalf("skipped startTick released the tickQ tag, it should still be held")
			}
			if _, live := s.tryRun("tickUnAckQ:guard-key"); live {
				t.Fatalf("skipped startTickUnAckQ released the tag, it should still be held")
			}
		})
	}
}

func debugValueLabel(v string) string {
	if v == "" {
		return "off"
	}
	return "on"
}

// TestNewService_SameSvcNameReusesCollectors 验证同进程内构造两个同名 Svc(HOSTNAME 固定,
// 即 k8s pod 名相同时的真实场景)不再 panic, 而是复用已注册的 collector, 打点落到同一条 series.
func TestNewService_SameSvcNameReusesCollectors(t *testing.T) {
	t.Setenv("HOSTNAME", itPodName(t))
	t.Setenv("DELAYTASK_REDIS_CLUSTER", "")
	t.Setenv("DELAYTASK_TICK_COUNT", "")
	t.Setenv("DELAYTASK_TICK_INTERVAL_MS", "")
	t.Setenv("DELAYTASK_CONSUME_LIMIT", "")

	name := itSvcName(t)
	first := NewService(name, offlineRedis())
	second := NewService(name, offlineRedis())

	if first.exp == second.exp {
		t.Fatalf("two Svc should keep their own Exporter, only the prometheus collector may be shared")
	}

	first.exp.Counter("publish_total").Add(2)
	second.exp.Counter("publish_total").Add(3)
	if got := itMetric(t, name, "publish_total", nil); got != 5 {
		t.Fatalf("both Svc should feed the same series, expected 5, got %+v", got)
	}

	first.exp.Gauge("tick_count").Set(11)
	second.exp.Gauge("tick_count").Set(22)
	if got := itMetric(t, name, "tick_count", nil); got != 22 {
		t.Fatalf("shared gauge expected the latest write 22, got %+v", got)
	}

	if n := countMetricLines(t, name, "publish_total", nil); n != 1 {
		t.Fatalf("expected a single publish_total series for the shared svcName, got %d", n)
	}
}

func TestEnvSnapshot(t *testing.T) {
	keys := []string{
		"DELAYTASK_TICK_COUNT", "DELAYTASK_TICK_INTERVAL_MS", "DELAYTASK_CONSUME_LIMIT",
		"DELAYTASK_UNACK_TIMEOUT_SEC", "DELAYTASK_JOB_TIMEOUT_SEC", "DELAYTASK_REDIS_CLUSTER",
		"REDELAY_DEBUG", "HOSTNAME",
	}
	for _, k := range keys {
		t.Logf("%s=%q", k, os.Getenv(k))
	}
}

func TestGetLogger_ConcurrentInitIsRaceFree(t *testing.T) {
	prev := logger
	logger = nil
	defer func() { logger = prev }()

	var wg sync.WaitGroup
	loggers := make([]*logrus.Logger, 32)
	for i := 0; i < len(loggers); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			loggers[n] = GetLogger()
		}(i)
	}
	wg.Wait()

	for i, l := range loggers {
		if l == nil {
			t.Fatalf("goroutine %d got a nil logger", i)
		}
		if l != loggers[0] {
			t.Fatalf("goroutine %d got a different logger instance, init is not serialized", i)
		}
	}
	t.Logf("并发 GetLogger 拿到同一个实例, -race 下无 data race")
}

func TestCtxLogger_ConcurrentUseIsRaceFree(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			GetLogger().Infof("concurrent logger write %d", n)
			CtxLogger(newCtxWithOpId(fmt.Sprintf("op-%d", n))).Infof("concurrent ctx log")
		}(i)
	}
	wg.Wait()
}
