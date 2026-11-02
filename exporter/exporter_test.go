package exporter

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var metricsSrv *httptest.Server

const itExporterPod = "pod-exporter-it"

func TestNew_ConstLabels(t *testing.T) {
	t.Setenv("HOSTNAME", "redelay-pod-7")

	e := New("svc-a")
	if e == nil {
		t.Fatalf("New returned nil")
	}
	if e.service != "svc-a" {
		t.Fatalf("service expected svc-a, got %s", e.service)
	}

	want := map[string]string{"service": "svc-a", "type": "delay-task", "hostname": "redelay-pod-7"}
	if len(e.constLabels) != len(want) {
		t.Fatalf("constLabels expected %+v, got %+v", want, e.constLabels)
	}
	for k, v := range want {
		if e.constLabels[k] != v {
			t.Fatalf("constLabel %s expected %s, got %s", k, v, e.constLabels[k])
		}
	}
	if e.counterSlot == nil || e.gaugeSlot == nil {
		t.Fatalf("collector slots not initialized")
	}
}

func TestNew_HostnameFallbackIsUnique(t *testing.T) {
	t.Setenv("HOSTNAME", "")

	a := New("svc-hostname")
	b := New("svc-hostname")

	for _, e := range []*Exporter{a, b} {
		h := e.constLabels["hostname"]
		if h == "" {
			t.Fatalf("hostname should never be empty, got %+v", e.constLabels)
		}
		if len(h) != 32 {
			t.Fatalf("fallback hostname expected 32 hex chars, got %d: %s", len(h), h)
		}
	}
	if a.constLabels["hostname"] == b.constLabels["hostname"] {
		t.Fatalf("fallback hostname should be unique per exporter, both got %s", a.constLabels["hostname"])
	}
}

func TestCounter_SameCollectorPerName(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-counter")

	e := New("svc-counter")
	c1 := e.Counter("it_counter_same")
	c2 := e.Counter("it_counter_same", "help of first call wins")
	if c1 != c2 {
		t.Fatalf("same metric name should return the cached collector")
	}

	c1.Inc()
	c1.Add(2)
	if got := metricValue(t, "it_counter_same", nil); got != 3 {
		t.Fatalf("counter expected 3, got %+v", got)
	}
	if got := metricValue(t, "it_counter_same", nil); got != 3 {
		t.Fatalf("cached counter should share the value, got %+v", got)
	}

	c3 := e.Counter("it_counter_other")
	if c3 == prometheus.Counter(c1) {
		t.Fatalf("different metric name should get a different collector")
	}
}

func TestGauge_SetAndDedupe(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-gauge")

	e := New("svc-gauge")
	g1 := e.Gauge("it_gauge_value")
	g2 := e.Gauge("it_gauge_value")
	if g1 != g2 {
		t.Fatalf("same gauge name should return the cached collector")
	}

	g1.Set(3.5)
	if got := metricValue(t, "it_gauge_value", nil); got != 3.5 {
		t.Fatalf("gauge expected 3.5, got %+v", got)
	}
	g1.Add(1)
	if got := metricValue(t, "it_gauge_value", nil); got != 4.5 {
		t.Fatalf("gauge expected 4.5 after Add, got %+v", got)
	}
	g1.Set(0)
	if got := metricValue(t, "it_gauge_value", nil); got != 0 {
		t.Fatalf("gauge expected 0, got %+v", got)
	}
}

func TestCounterWith_LabelIsolation(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-labels")

	e := New("svc-labels")
	k1 := e.CounterWith("it_counter_labels", map[string]string{"key": "k-1"})
	k2 := e.CounterWith("it_counter_labels", map[string]string{"key": "k-2"})
	if k1 == k2 {
		t.Fatalf("different label values should get different collectors")
	}

	sameK1 := e.CounterWith("it_counter_labels", map[string]string{"key": "k-1"})
	if sameK1 != k1 {
		t.Fatalf("same labels should reuse the cached collector")
	}

	k1.Inc()
	k1.Inc()
	k2.Inc()
	if got := metricValue(t, "it_counter_labels", map[string]string{"key": "k-1"}); got != 2 {
		t.Fatalf("k-1 counter expected 2, got %+v", got)
	}
	if got := metricValue(t, "it_counter_labels", map[string]string{"key": "k-2"}); got != 1 {
		t.Fatalf("k-2 counter expected 1, got %+v", got)
	}

	if n := countMetricLines(t, "it_counter_labels", map[string]string{"key": "k-1"}); n != 1 {
		t.Fatalf("expected exactly 1 sample for key=k-1, got %d", n)
	}
}

func TestCounterWith_LabelOrderIndependent(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-order")

	e := New("svc-order")
	a := e.CounterWith("it_counter_order", map[string]string{"key": "k", "zone": "cn"})
	b := e.CounterWith("it_counter_order", map[string]string{"zone": "cn", "key": "k"})
	if a != b {
		t.Fatalf("slot key must be label-order independent")
	}
	if got := slotKey("n", map[string]string{"b": "2", "a": "1"}); got != "n|a=1|b=2" {
		t.Fatalf("slotKey expected n|a=1|b=2, got %s", got)
	}
	if got := slotKey("n", nil); got != "n" {
		t.Fatalf("slotKey without labels expected n, got %s", got)
	}
	if got := slotKey("n", map[string]string{}); got != "n" {
		t.Fatalf("slotKey with empty labels expected n, got %s", got)
	}
}

func TestMergeLabels(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-merge")

	e := New("svc-merge")

	merged := e.mergeLabels(nil)
	if len(merged) != len(e.constLabels) {
		t.Fatalf("nil labels should return constLabels, got %+v", merged)
	}

	withLabels := e.mergeLabels(map[string]string{"key": "k-1"})
	if withLabels["service"] != "svc-merge" || withLabels["type"] != "delay-task" || withLabels["hostname"] != "pod-merge" {
		t.Fatalf("const labels should be merged in, got %+v", withLabels)
	}
	if withLabels["key"] != "k-1" {
		t.Fatalf("custom label missing, got %+v", withLabels)
	}

	override := e.mergeLabels(map[string]string{"type": "custom", "service": "override"})
	if override["type"] != "custom" || override["service"] != "override" {
		t.Fatalf("custom labels should win over const labels, got %+v", override)
	}
	if e.constLabels["type"] != "delay-task" || e.constLabels["service"] != "svc-merge" {
		t.Fatalf("mergeLabels must not mutate constLabels, got %+v", e.constLabels)
	}
}

func TestGaugeWith_LabelIsolation(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-gaugelabel")

	e := New("svc-gaugelabel")
	g1 := e.GaugeWith("it_gauge_labels", map[string]string{"key": "k-1"})
	g2 := e.GaugeWith("it_gauge_labels", map[string]string{"key": "k-2"})
	g1.Set(1)
	g2.Set(2)

	if got := metricValue(t, "it_gauge_labels", map[string]string{"key": "k-1"}); got != 1 {
		t.Fatalf("k-1 gauge expected 1, got %+v", got)
	}
	if got := metricValue(t, "it_gauge_labels", map[string]string{"key": "k-2"}); got != 2 {
		t.Fatalf("k-2 gauge expected 2, got %+v", got)
	}
	if e.GaugeWith("it_gauge_labels", map[string]string{"key": "k-1"}) != g1 {
		t.Fatalf("same labels should reuse the cached gauge")
	}
}

func TestCollectorsRegisteredWithFullLabels(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-registered")

	e := New("svc-registered")
	e.CounterWith("it_registered_counter", map[string]string{"key": "k-9"}).Inc()
	e.GaugeWith("it_registered_gauge", map[string]string{"key": "k-9"}).Set(7)

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather failed: %s", err)
	}

	found := map[string]bool{}
	for _, f := range families {
		if f.GetName() != "it_registered_counter" && f.GetName() != "it_registered_gauge" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["service"] != "svc-registered" || labels["type"] != "delay-task" ||
				labels["hostname"] != "pod-registered" || labels["key"] != "k-9" {
				t.Fatalf("metric %s has wrong label set: %+v", f.GetName(), labels)
			}
			found[f.GetName()] = true
		}
	}
	if !found["it_registered_counter"] || !found["it_registered_gauge"] {
		t.Fatalf("expected both collectors in the default registry, found %+v", found)
	}
}

func TestMetricsExposedByPromhttp(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-http")

	e := New("svc-http")
	e.Counter("it_http_publish_total").Add(4)
	e.GaugeWith("it_http_buffer_queue_length", map[string]string{"key": "k-1"}).Set(9)

	srv := httptest.NewServer(promhttp.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics status expected 200, got %d", resp.StatusCode)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body failed: %s", err)
	}
	body := string(b)

	for _, want := range []string{
		`it_http_publish_total{hostname="pod-http",service="svc-http",type="delay-task"} 4`,
		`it_http_buffer_queue_length{hostname="pod-http",key="k-1",service="svc-http",type="delay-task"} 9`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q", want)
		}
	}
}

func TestConcurrentCollectorCreation(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-concurrent")

	e := New("svc-concurrent")
	done := make(chan struct{})
	for i := 0; i < 16; i++ {
		go func(n int) {
			for j := 0; j < 50; j++ {
				e.CounterWith("it_concurrent_counter", map[string]string{"key": "k"}).Inc()
				e.GaugeWith("it_concurrent_gauge", map[string]string{"key": "k"}).Set(float64(j))
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 16; i++ {
		<-done
	}

	if got := metricValue(t, "it_concurrent_counter", nil); got != 16*50 {
		t.Fatalf("concurrent counter expected %d, got %+v", 16*50, got)
	}
	if got := metricValue(t, "it_concurrent_gauge", nil); got != 49 {
		t.Fatalf("concurrent gauge expected 49, got %+v", got)
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

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics body failed, err: %s", err)
	}
	return string(b)
}

func metricLines(body, name string) []string {
	prefix := name + "{"
	out := make([]string, 0, 8)
	for _, l := range strings.Split(body, "\n") {
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

func parseMetricValue(t *testing.T, line string) float64 {
	t.Helper()

	idx := strings.LastIndex(line, " ")
	if idx < 0 {
		t.Fatalf("unparsable metric line: %s", line)
	}
	v, err := strconv.ParseFloat(line[idx+1:], 64)
	if err != nil {
		t.Fatalf("unparsable metric value, line: %s, err: %s", line, err)
	}
	return v
}

func metricValue(t *testing.T, name string, extra map[string]string) float64 {
	t.Helper()

	lines := metricLines(scrapeBody(t), name)
	if len(lines) == 0 {
		t.Fatalf("metric %s is not exposed by /metrics", name)
	}

	matched := 0
	total := float64(0)
	for _, line := range lines {
		if !matchLabels(line, extra) {
			continue
		}
		matched++
		total += parseMetricValue(t, line)
	}
	if matched == 0 {
		t.Fatalf("metric %s with labels %+v is not exposed", name, extra)
	}
	return total
}

func countMetricLines(t *testing.T, name string, extra map[string]string) int {
	t.Helper()

	n := 0
	for _, line := range metricLines(scrapeBody(t), name) {
		if matchLabels(line, extra) {
			n++
		}
	}
	return n
}

// TestCounterWith_ReusesRegisteredCollector covers the AlreadyRegisteredError branch:
// two exporters of the same service share the collector instead of panicking.
func TestCounterWith_ReusesRegisteredCollector(t *testing.T) {
	t.Setenv("HOSTNAME", itExporterPod)

	a := New("svc-reuse-counter")
	b := New("svc-reuse-counter")

	if a.counterSlot == nil || b.counterSlot == nil {
		t.Fatalf("slots not initialized")
	}

	a.Counter("it_reuse_counter").Add(2)
	b.Counter("it_reuse_counter").Add(3)

	if got := metricValue(t, "it_reuse_counter", map[string]string{"service": "svc-reuse-counter"}); got != 5 {
		t.Fatalf("both exporters should feed the same series, expected 5, got %+v", got)
	}
	if n := countMetricLines(t, "it_reuse_counter", map[string]string{"service": "svc-reuse-counter"}); n != 1 {
		t.Fatalf("expected one series, got %d", n)
	}

	first := a.counterSlot[slotKey("it_reuse_counter", a.constLabels)]
	second := b.counterSlot[slotKey("it_reuse_counter", b.constLabels)]
	if first != second {
		t.Fatalf("the second exporter should cache the already registered collector")
	}
}

func TestGaugeWith_ReusesRegisteredCollector(t *testing.T) {
	t.Setenv("HOSTNAME", itExporterPod)

	a := New("svc-reuse-gauge")
	b := New("svc-reuse-gauge")

	a.GaugeWith("it_reuse_gauge", map[string]string{"key": "k-1"}).Set(4)
	b.GaugeWith("it_reuse_gauge", map[string]string{"key": "k-1"}).Set(6)

	want := map[string]string{"service": "svc-reuse-gauge", "key": "k-1"}
	if got := metricValue(t, "it_reuse_gauge", want); got != 6 {
		t.Fatalf("shared gauge expected the latest write 6, got %+v", got)
	}
	if n := countMetricLines(t, "it_reuse_gauge", want); n != 1 {
		t.Fatalf("expected one series, got %d", n)
	}

	first := a.gaugeSlot[slotKey("it_reuse_gauge", a.mergeLabels(map[string]string{"key": "k-1"}))]
	second := b.gaugeSlot[slotKey("it_reuse_gauge", b.mergeLabels(map[string]string{"key": "k-1"}))]
	if first != second {
		t.Fatalf("the second exporter should cache the already registered gauge")
	}
}

// TestCounterWith_TypeClashReusesExistingCollector pins the actual behaviour when one
// name is used for two metric types: *prometheus.gauge also satisfies prometheus.Counter
// (it has Inc/Add), so the already registered gauge is reused and CounterWith writes land
// on it. The library never mixes types for one name; this documents the hazard.
func TestCounterWith_TypeClashReusesExistingCollector(t *testing.T) {
	t.Setenv("HOSTNAME", itExporterPod)

	svc := "svc-type-clash"
	a := New(svc)
	b := New(svc)

	a.Gauge("it_clash_metric").Set(9)
	c := b.Counter("it_clash_metric")
	c.Add(5)

	labels := map[string]string{"service": svc}
	if got := metricValue(t, "it_clash_metric", labels); got != 14 {
		t.Fatalf("the counter write lands on the registered gauge, expected 9+5=14, got %+v", got)
	}
	if n := countMetricLines(t, "it_clash_metric", labels); n != 1 {
		t.Fatalf("expected one series, got %d", n)
	}
	body := scrapeBody(t)
	foundType := false
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "# TYPE it_clash_metric ") {
			foundType = true
			if !strings.HasSuffix(line, "gauge") {
				t.Fatalf("the series should keep the first registered type gauge, got: %s", line)
			}
		}
	}
	if !foundType {
		t.Fatalf("it_clash_metric is not exposed with a TYPE line")
	}

	if a.gaugeSlot[slotKey("it_clash_metric", a.constLabels)] != b.counterSlot[slotKey("it_clash_metric", b.constLabels)] {
		t.Fatalf("both exporters should hold the same collector")
	}
}

// TestCounterWith_InconsistentLabelsDegrades covers the non-AlreadyRegistered
// registration error: it is logged and the collector keeps working locally.
func TestCounterWith_InconsistentLabelsDegrades(t *testing.T) {
	t.Setenv("HOSTNAME", itExporterPod)

	svc := "svc-inconsistent"
	a := New(svc)
	b := New(svc)

	a.CounterWith("it_inconsistent_total", map[string]string{"key": "k-1"}).Inc()
	c := b.CounterWith("it_inconsistent_total", map[string]string{"zone": "cn"})
	c.Add(7)

	if got := metricValue(t, "it_inconsistent_total", map[string]string{"service": svc, "key": "k-1"}); got != 1 {
		t.Fatalf("the first series should be untouched, got %+v", got)
	}

	body := scrapeBody(t)
	for _, line := range metricLines(body, "it_inconsistent_total") {
		if matchLabels(line, map[string]string{"service": svc, "zone": "cn"}) {
			t.Fatalf("the inconsistent collector must not be registered, but it is exposed: %s", line)
		}
	}
	if b.counterSlot[slotKey("it_inconsistent_total", b.mergeLabels(map[string]string{"zone": "cn"}))] != c {
		t.Fatalf("the degraded collector should still be cached in its own exporter")
	}
}

// TestCounter_InvalidHelpStaysLocal pins that a registration failure never panics.
func TestGauge_DeclinedRegistrationDoesNotPanic(t *testing.T) {
	t.Setenv("HOSTNAME", itExporterPod)

	svc := "svc-declined"
	a := New(svc)
	b := New(svc)

	a.GaugeWith("it_declined_gauge", map[string]string{"key": "k"}).Set(1)
	g := b.GaugeWith("it_declined_gauge", map[string]string{"key": "other"})
	g.Set(2)

	want := map[string]string{"service": svc, "key": "other"}
	if n := countMetricLines(t, "it_declined_gauge", want); n != 1 {
		t.Fatalf("a different label value is a new series, expected 1, got %d", n)
	}
	if got := metricValue(t, "it_declined_gauge", want); got != 2 {
		t.Fatalf("the new series expected 2, got %+v", got)
	}
}
