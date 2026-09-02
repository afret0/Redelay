package exporter

import (
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/afret0/wheel/tool"
	"github.com/prometheus/client_golang/prometheus"
)

type Exporter struct {
	service     string
	constLabels map[string]string

	mx sync.Mutex

	counterSlot map[string]prometheus.Counter
	gaugeSlot   map[string]prometheus.Gauge
}

func New(svc string) *Exporter {

	//ex.service = svc

	hostname := os.Getenv("HOSTNAME")
	if hostname == "" {
		hostname = tool.UUIDWithoutHyphen()
	}

	ex := &Exporter{
		service: svc,

		constLabels: map[string]string{"service": svc, "type": "delay-task", "hostname": hostname},

		counterSlot: make(map[string]prometheus.Counter),
		gaugeSlot:   make(map[string]prometheus.Gauge),

		mx: sync.Mutex{},
	}

	return ex
}

func (e *Exporter) Counter(name string, helpChain ...string) prometheus.Counter {
	return e.CounterWith(name, nil, helpChain...)
}

func (e *Exporter) CounterWith(name string, labels map[string]string, helpChain ...string) prometheus.Counter {
	help := ""
	if len(helpChain) != 0 {
		help = helpChain[0]
	}

	merged := e.mergeLabels(labels)
	slotKey := slotKey(name, merged)

	e.mx.Lock()
	defer e.mx.Unlock()
	C, ok := e.counterSlot[slotKey]
	if !ok {
		C = prometheus.NewCounter(prometheus.CounterOpts{
			Name:        name,
			Help:        help,
			ConstLabels: merged,
		})

		e.counterSlot[slotKey] = C

		prometheus.MustRegister(C)
	}

	return C
}

func (e *Exporter) Gauge(name string, helpChain ...string) prometheus.Gauge {
	return e.GaugeWith(name, nil, helpChain...)
}

func (e *Exporter) GaugeWith(name string, labels map[string]string, helpChain ...string) prometheus.Gauge {
	help := ""
	if len(helpChain) != 0 {
		help = helpChain[0]
	}

	merged := e.mergeLabels(labels)
	slotKey := slotKey(name, merged)

	e.mx.Lock()
	defer e.mx.Unlock()
	G, ok := e.gaugeSlot[slotKey]

	if !ok {

		G = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        name,
			Help:        help,
			ConstLabels: merged,
		})

		e.gaugeSlot[slotKey] = G

		prometheus.MustRegister(G)

	}

	return G
}

func (e *Exporter) mergeLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return e.constLabels
	}

	merged := make(map[string]string, len(e.constLabels)+len(labels))
	for k, v := range e.constLabels {
		merged[k] = v
	}
	for k, v := range labels {
		merged[k] = v
	}

	return merged
}

func slotKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString(name)
	for _, k := range keys {
		sb.WriteString("|")
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(labels[k])
	}

	return sb.String()
}
