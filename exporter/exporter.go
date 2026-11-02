package exporter

import (
	"errors"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/afret0/wheel/log"
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

		if err := prometheus.Register(C); err != nil {
			var already prometheus.AlreadyRegisteredError
			switch {
			case errors.As(err, &already):
				// 同进程内同名同 label 的 collector 已注册, 复用它, 两边的打点落到同一条 series,
				// 而不是让第二个服务构建时 panic.
				// 注意 *prometheus.gauge 同时实现了 prometheus.Counter(有 Inc/Add), 所以名字被
				// 另一种类型占用时这里也会复用它, CounterWith 的写入会落到那条 gauge 上:
				// 同一个 name 不要跨 Counter/Gauge 混用.
				if existed, castOk := already.ExistingCollector.(prometheus.Counter); castOk {
					C = existed
				}
			default:
				log.GetLogger().Errorf("register counter failed, name: %s, labels: %+v, err: %s", name, merged, err)
			}
		}

		e.counterSlot[slotKey] = C
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

		if err := prometheus.Register(G); err != nil {
			var already prometheus.AlreadyRegisteredError
			switch {
			case errors.As(err, &already):
				if existed, castOk := already.ExistingCollector.(prometheus.Gauge); castOk {
					G = existed
				}
			default:
				log.GetLogger().Errorf("register gauge failed, name: %s, labels: %+v, err: %s", name, merged, err)
			}
		}

		e.gaugeSlot[slotKey] = G
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
