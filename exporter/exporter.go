package exporter

import (
	"os"
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
	help := ""
	if len(helpChain) != 0 {
		help = helpChain[0]
	}
	e.mx.Lock()
	defer e.mx.Unlock()
	C, ok := e.counterSlot[name]
	if !ok {
		C = prometheus.NewCounter(prometheus.CounterOpts{
			Name:        name,
			Help:        help,
			ConstLabels: e.constLabels,
		})

		e.counterSlot[name] = C

		prometheus.MustRegister(C)
	}

	return C
}

func (e *Exporter) Gauge(name string, helpChain ...string) prometheus.Gauge {
	help := ""
	if len(helpChain) != 0 {
		help = helpChain[0]
	}
	e.mx.Lock()
	defer e.mx.Unlock()
	G, ok := e.gaugeSlot[name]

	if !ok {

		G = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        name,
			Help:        help,
			ConstLabels: e.constLabels,
		})

		e.gaugeSlot[name] = G

		prometheus.MustRegister(G)

	}

	return G
}
