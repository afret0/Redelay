package Redelay

import (
	"Redelay/exporter"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

var RetryErr = fmt.Errorf("retry")
var lg = GetLogger()

//const ExporterBufferQueueLength = "buffer_queue_length"

type Service struct {
	redis   redis.UniversalClient
	svcName string
	slot    map[string]func(p string) error
	mx      sync.RWMutex

	key         string
	unAckKey    string
	bufferQueue string

	lock *redislock.Client

	tickQCount   int64
	tickInterval int64

	consumeLimit int64

	exp *exporter.Exporter

	//antsPool *ants.Pool
}

type event struct {
	Id              string `json:"id"`
	Name            string `json:"name"`
	Args            string `json:"args"`
	RetryCount      int64  `json:"retryCount"`
	UnAckRetryCount int64  `json:"unAckRetryCount"`
}

func NewService(svcName string, redis redis.UniversalClient) *Service {
	if svcName == "" {
		panic("svcName is required")
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

	//antsPoolSize := 500
	//envAntsPoolSizeS := os.Getenv("DELAYTASK_ANTS_POOL_SIZE")
	//envAntsPoolSize := tool.ConStringToInt64WithoutErr(envAntsPoolSizeS)
	//if envAntsPoolSize != 0 {
	//	antsPoolSize = int(envAntsPoolSize)
	//}
	//
	//antsPool, err := ants.NewPool(antsPoolSize)
	//if err != nil {
	//	panic(err)
	//}
	//defer antsPool.Release()

	//lg.Infof("count: %d, tickInterval: %d, consumeLimit: %d, antsPoolSize: %d", count, tickInterval, consumeLimit, antsPoolSize)
	lg.Infof("count: %d, tickInterval: %d, consumeLimit: %d", count, tickInterval, consumeLimit)

	svr := &Service{
		redis:   redis,
		svcName: svcName,
		slot:    make(map[string]func(p string) error),
		mx:      sync.RWMutex{},

		key:         fmt.Sprintf("%s:Redelay", svcName),
		unAckKey:    fmt.Sprintf("%s:Redelay:unAck", svcName),
		bufferQueue: fmt.Sprintf("%s:Redelay:bufferQueue", svcName),

		lock: redislock.New(redis),

		tickQCount:   count,
		tickInterval: tickInterval,

		consumeLimit: consumeLimit,

		exp: exporter.New(svcName),

		//antsPool: antsPool,
	}

	svr.exp.Gauge("tick_count").Set(float64(svr.tickQCount))
	svr.exp.Gauge("tick_interval_ms").Set(float64(svr.tickInterval))
	svr.exp.Gauge("consume_limit").Set(float64(svr.consumeLimit))
	//svr.exp.Gauge("ants_pool_size").Set(float64(antsPoolSize))

	//go svr.startTick()
	//go svr.startConsume()
	//
	//go svr.loopFlushExp()

	return svr
}

func (s *Service) Debug() bool {
	return tool.EnvEnabled("DELAYTASK_DEBUG")
}

func (s *Service) loopFlushExp() {

	lg.Infof("flush exp start...")
	s.exp.Gauge("loop_flush_exp_ping").Set(1)

	defer func() {
		s.exp.Gauge("loop_flush_exp_ping").Set(0)
		lg.Infof("loopFlushExp stop...")
	}()

	ctx := tool.NewCtxBK()

	for _ = range time.Tick(5 * time.Second) {
		bufferQLen, _ := s.redis.LLen(ctx, s.bufferQueue).Uint64()
		s.exp.Gauge("buffer_queue_length").Set(float64(bufferQLen))

		unAckQlen, _ := s.redis.ZCard(ctx, s.unAckKey).Uint64()
		s.exp.Gauge("unAck_queue_length").Set(float64(unAckQlen))

		ackQlen, _ := s.redis.ZCard(ctx, s.key).Uint64()
		s.exp.Gauge("ack_queue_length").Set(float64(ackQlen))

		redisPing, err := s.redis.Ping(ctx).Result()
		if err != nil {
			s.exp.Gauge("redis_ping").Set(0)
		}
		if redisPing == "PONG" {
			s.exp.Gauge("redis_ping").Set(1)
		}
	}
}
