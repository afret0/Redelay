package delayTask

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/afret0/delayTask/exporter"
	"github.com/afret0/wheel/tool"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

var RetryErr = fmt.Errorf("retry")
var lg = GetLogger()

//const ExporterBufferQueueLength = "buffer_queue_length"

type Service struct {
	redis  redis.UniversalClient
	caller string
	slot   map[string]func(p string) error
	mx     sync.RWMutex

	key         string
	unAckKey    string
	bufferQueue string

	init bool
	lock *redislock.Client

	tickQCount   int64
	tickInterval int64

	consumeLimit int64

	exp *exporter.Exporter
}

type event struct {
	Id              string `json:"id"`
	Name            string `json:"name"`
	Args            string `json:"args"`
	RetryCount      int64  `json:"retryCount"`
	UnAckRetryCount int64  `json:"unAckRetryCount"`
}

func NewService(caller string, redis redis.UniversalClient) *Service {
	if caller == "" {
		panic("caller is required")
	}

	count := int64(500)
	envCountS := os.Getenv("DELAYTASK_TICK_COUNT")
	envCount := tool.ConStringToInt64WithoutErr(envCountS)
	if envCount != 0 {
		count = envCount
	}

	tickInterval := int64(500)
	envTickIntervalS := os.Getenv("DELAYTASK_TICK_INTERVAL_MS")
	envTickInterval := tool.ConStringToInt64WithoutErr(envTickIntervalS)
	if envTickInterval != 0 {
		tickInterval = envTickInterval
	}

	consumeLimit := count * 5
	envConsumeLimitS := os.Getenv("DELAYTASK_CONSUME_LIMIT")
	envConsumeLimit := tool.ConStringToInt64WithoutErr(envConsumeLimitS)
	if envConsumeLimit != 0 {
		consumeLimit = envConsumeLimit
	}

	lg.Infof("count: %d, envCountS: %s, envCount: %d, consumeLimit: %d", count, envCountS, envCount, consumeLimit)

	svr := &Service{
		redis:  redis,
		caller: caller,
		slot:   make(map[string]func(p string) error),
		mx:     sync.RWMutex{},

		key:         fmt.Sprintf("%s:delayTask", caller),
		unAckKey:    fmt.Sprintf("%s:delayTask:unAck", caller),
		bufferQueue: fmt.Sprintf("%s:delayTask:bufferQueue", caller),

		init: true,
		lock: redislock.New(redis),

		tickQCount:   count,
		tickInterval: tickInterval,

		consumeLimit: consumeLimit,

		exp: exporter.New(caller),
	}

	svr.exp.Gauge("tick_count").Set(float64(svr.tickQCount))
	svr.exp.Gauge("tick_interval_ms").Set(float64(svr.tickInterval))
	svr.exp.Gauge("consume_limit").Set(float64(svr.consumeLimit))

	go svr.startTick()
	go svr.startConsume()

	go svr.loopFlushExp()

	return svr
}

func (s *Service) Debug() bool {
	return tool.EnvEnabled("DELAYTASK_DEBUG")
}

func (s *Service) loopFlushExp() {

	s.exp.Gauge("loop_flush_exp_ping").Set(1)
	defer s.exp.Gauge("loop_flush_exp_ping").Set(0)

	ctx := tool.NewCtxBK()

	for _ = range time.Tick(5 * time.Second) {
		bufferQLen, _ := s.redis.LLen(ctx, s.bufferQueue).Uint64()
		s.exp.Gauge("buffer_queue_length").Set(float64(bufferQLen))

		unAckQlen, _ := s.redis.ZCard(ctx, s.unAckKey).Uint64()
		s.exp.Gauge("unAck_queue_length").Set(float64(unAckQlen))

		ackQlen, _ := s.redis.ZCard(ctx, s.key).Uint64()
		s.exp.Gauge("ack_queue_length").Set(float64(ackQlen))
	}
}
