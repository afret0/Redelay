package Redelay

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var metricsSrv *httptest.Server

const redisAddrEnv = "REDELAY_REDIS_ADDR"

func testRedis(t *testing.T) redis.UniversalClient {
	t.Helper()

	addr := os.Getenv(redisAddrEnv)
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	rc := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{addr}})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rc.Ping(ctx).Err(); err != nil {
		_ = rc.Close()
		t.Skipf("redis %s unavailable, skip integration test, err: %s", addr, err)
	}

	t.Cleanup(func() { _ = rc.Close() })
	cleanupRedisKeys(t, rc, itSvcName(t)+"*")
	t.Cleanup(func() { cleanupRedisKeys(t, rc, itSvcName(t)+"*") })

	return rc
}

func offlineRedis() redis.UniversalClient {
	return redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"127.0.0.1:1"}})
}

func newCtxWithOpId(opId string) context.Context {
	return context.WithValue(context.Background(), "opId", opId)
}

func itSvcName(t *testing.T) string {
	return "redelay-it-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
}

func cleanupRedisKeys(t *testing.T, rc redis.UniversalClient, pattern string) {
	t.Helper()

	ctx := context.Background()
	iter := rc.Scan(ctx, 0, pattern, 500).Iterator()
	keys := make([]string, 0, 64)
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		t.Errorf("scan %s failed, err: %s", pattern, err)
		return
	}
	if len(keys) == 0 {
		return
	}
	if err := rc.Del(ctx, keys...).Err(); err != nil {
		t.Errorf("del %d keys failed, err: %s", len(keys), err)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("wait timeout(%s), %s", timeout, desc)
}

type queueState struct {
	delay     int64
	buffer    int64
	unAck     int64
	delayRaw  []string
	bufferRaw []string
	unAckRaw  []string
}

func (q queueState) drained() bool {
	return q.delay == 0 && q.buffer == 0 && q.unAck == 0
}

func (q queueState) String() string {
	return fmt.Sprintf("delay: %d, buffer: %d, unAck: %d", q.delay, q.buffer, q.unAck)
}

func readQueues(t *testing.T, rc redis.UniversalClient, s *Svc, key string) queueState {
	t.Helper()

	ctx := context.Background()
	st := queueState{}

	members, err := rc.ZRangeWithScores(ctx, s.key, 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange %s failed, err: %s", s.key, err)
	}
	st.delay = int64(len(members))
	for _, m := range members {
		st.delayRaw = append(st.delayRaw, fmt.Sprintf("%v", m.Member))
	}

	buffer, err := rc.LRange(ctx, s.bufferQName(key), 0, -1).Result()
	if err != nil {
		t.Fatalf("lrange %s failed, err: %s", s.bufferQName(key), err)
	}
	st.buffer = int64(len(buffer))
	st.bufferRaw = buffer

	unAck, err := rc.ZRange(ctx, s.unAckQName(key), 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange %s failed, err: %s", s.unAckQName(key), err)
	}
	st.unAck = int64(len(unAck))
	st.unAckRaw = unAck

	return st
}

func mustEventJSON(t *testing.T, e *event) string {
	t.Helper()

	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal event failed, err: %s", err)
	}
	return string(b)
}

func unmarshalEvent(t *testing.T, s string) *event {
	t.Helper()

	e := new(event)
	if err := json.Unmarshal([]byte(s), e); err != nil {
		t.Fatalf("unmarshal event %s failed, err: %s", s, err)
	}
	return e
}

func seedUnAck(t *testing.T, rc redis.UniversalClient, s *Svc, key, eventS string, score int64) {
	t.Helper()

	ctx := context.Background()
	err := rc.ZAdd(ctx, s.unAckQName(key), redis.Z{Score: float64(score), Member: eventS}).Err()
	if err != nil {
		t.Fatalf("zadd %s failed, err: %s", s.unAckQName(key), err)
	}
}

func TestMain(m *testing.M) {
	metricsSrv = httptest.NewServer(promhttp.Handler())
	defer metricsSrv.Close()
	os.Exit(m.Run())
}

func scrapeBody(t *testing.T) string {
	t.Helper()

	resp, err := http.Get(metricsSrv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed, err: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics status expected 200, got %d", resp.StatusCode)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics body failed, err: %s", err)
	}
	return string(b)
}

func metricLines(body, name string) []string {
	prefix := name + "{"
	lines := strings.Split(body, "\n")

	out := make([]string, 0, 8)
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, prefix) {
			out = append(out, l)
		}
	}
	return out
}

func matchLabels(line string, labels map[string]string) bool {
	for k, v := range labels {
		if !strings.Contains(line, fmt.Sprintf("%s=%q", k, v)) {
			return false
		}
	}
	return true
}

func parseMetricValue(line string) float64 {
	idx := strings.LastIndex(line, " ")
	if idx < 0 {
		panic("unparsable metric line: " + line)
	}
	v, err := strconv.ParseFloat(line[idx+1:], 64)
	if err != nil {
		panic("unparsable metric value: " + line)
	}
	return v
}

func sumMetric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()

	total := float64(0)
	for _, line := range metricLines(scrapeBody(t), name) {
		if matchLabels(line, labels) {
			total += parseMetricValue(line)
		}
	}
	return total
}

func svcLabels(svcName string, extra map[string]string) map[string]string {
	labels := map[string]string{"service": svcName}
	for k, v := range extra {
		labels[k] = v
	}
	return labels
}

func itMetric(t *testing.T, svcName, name string, extra map[string]string) float64 {
	t.Helper()

	body := scrapeBody(t)
	want := svcLabels(svcName, extra)

	matched := 0
	total := float64(0)
	for _, line := range metricLines(body, name) {
		if !matchLabels(line, want) {
			continue
		}
		matched++
		total += parseMetricValue(line)
	}
	if matched == 0 {
		t.Fatalf("metric %s with labels %+v is not exposed by /metrics", name, want)
	}
	return total
}

func waitMetric(t *testing.T, svcName, name string, extra map[string]string, want float64, timeout time.Duration) {
	t.Helper()

	waitUntil(t, timeout, fmt.Sprintf("metric %s == %+v (labels %+v)", name, want, extra), func() bool {
		body := scrapeBodyNoFatal(name)
		if body == "" {
			return false
		}
		matched := 0
		total := float64(0)
		req := svcLabels(svcName, extra)
		for _, line := range metricLines(body, name) {
			if !matchLabels(line, req) {
				continue
			}
			matched++
			total += parseMetricValue(line)
		}
		return matched > 0 && total == want
	})
}

func scrapeBodyNoFatal(metric string) string {
	resp, err := http.Get(metricsSrv.URL + "/metrics")
	if err != nil {
		GetLogger().Errorf("scrape metrics for %s failed, err: %s", metric, err)
		return ""
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		GetLogger().Errorf("read metrics body for %s failed, err: %s", metric, err)
		return ""
	}
	return string(b)
}

func countMetricLines(t *testing.T, svcName, name string, extra map[string]string) int {
	t.Helper()

	req := svcLabels(svcName, extra)
	n := 0
	for _, line := range metricLines(scrapeBody(t), name) {
		if matchLabels(line, req) {
			n++
		}
	}
	return n
}
