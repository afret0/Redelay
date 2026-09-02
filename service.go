package Redelay

import (
	"Redelay/exporter"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

var RetryErr = fmt.Errorf("retry")

//var lg = GetLogger()

//const ExporterBufferQueueLength = "buffer_queue_length"

type Svc struct {
	redis   redis.UniversalClient
	svcName string

	key string

	unAckBufferQPrefix string
	bufferQPrefix      string

	lock *redislock.Client

	tickQCount   int64
	tickInterval int64

	// runTag 保证同名后台循环在当前进程内只启动一个.
	runTag sync.Map

	// jobKeys 记录本进程已 LaunchJob 的 key, 用于队列长度打点.
	jobKeys sync.Map

	unAckTimeout time.Duration
	jobTimeout   time.Duration

	consumeLimit int64

	exp *exporter.Exporter

	//antsPool *ants.Pool
}

type event struct {
	Id              string `json:"id"`
	Key             string `json:"key"`
	Args            string `json:"args"`
	RetryCount      int64  `json:"retryCount"`
	UnAckRetryCount int64  `json:"unAckRetryCount"`
}

func NewService(svcName string, redis redis.UniversalClient) *Svc {
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

	unAckTimeout := 3 * time.Minute
	envUnAckTimeoutS := os.Getenv("DELAYTASK_UNACK_TIMEOUT_SEC")
	envUnAckTimeout := tool.ConStringToInt64WithoutErr(envUnAckTimeoutS)
	if envUnAckTimeout != 0 {
		unAckTimeout = time.Duration(envUnAckTimeout) * time.Second
	}

	jobTimeout := 60 * time.Second
	envJobTimeoutS := os.Getenv("DELAYTASK_JOB_TIMEOUT_SEC")
	envJobTimeout := tool.ConStringToInt64WithoutErr(envJobTimeoutS)
	if envJobTimeout != 0 {
		jobTimeout = time.Duration(envJobTimeout) * time.Second
	}

	// unAck 扫描器必须晚于任务硬超时触发, 否则任务还在跑就会被重投, 造成重复执行.
	if unAckTimeout <= jobTimeout {
		unAckTimeout = jobTimeout * 2
		GetLogger().Warnf("unAckTimeout must be greater than jobTimeout, adjusted to %s", unAckTimeout)
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

	// Lua 脚本会同时写延时 ZSET 和 bufferQ/unAck, Redis Cluster 下必须落在同一 slot,
	// 因此集群模式给三者套上相同的 hash tag. 注意开启后 key 名变化, 已有数据需要迁移.
	keyPrefix := fmt.Sprintf("%s:Redelay", svcName)
	if tool.EnvEnabled("DELAYTASK_REDIS_CLUSTER") {
		keyPrefix = fmt.Sprintf("{%s:Redelay}", svcName)
	}

	//lg.Infof("count: %d, tickInterval: %d, consumeLimit: %d, antsPoolSize: %d", count, tickInterval, consumeLimit, antsPoolSize)
	GetLogger().Infof("count: %d, tickInterval: %d, consumeLimit: %d", count, tickInterval, consumeLimit)

	svr := &Svc{
		redis:   redis,
		svcName: svcName,

		key:                keyPrefix,
		unAckBufferQPrefix: keyPrefix + ":unAck",
		bufferQPrefix:      keyPrefix + ":bufferQueue",

		lock: redislock.New(redis),

		tickQCount:   count,
		tickInterval: tickInterval,

		unAckTimeout: unAckTimeout,
		jobTimeout:   jobTimeout,

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

func (s *Svc) Debug() bool {
	return tool.EnvEnabled("REDELAY_DEBUG")
}

// tryRun 尝试占用 name 对应的后台循环, 返回 false 说明本进程内已经有一个在跑.
// 占用成功时返回的 release 用于退出时释放.
func (s *Svc) tryRun(name string) (release func(), ok bool) {
	if _, loaded := s.runTag.LoadOrStore(name, struct{}{}); loaded {
		return nil, false
	}
	return func() { s.runTag.Delete(name) }, true
}

func (s *Svc) loopFlushExp() {
	release, ok := s.tryRun("loopFlushExp")
	if !ok {
		return
	}
	defer release()

	GetLogger().Infof("flush exp start...")
	s.exp.Gauge("loop_flush_exp_ping").Set(1)

	defer func() {
		s.exp.Gauge("loop_flush_exp_ping").Set(0)
		GetLogger().Infof("loopFlushExp stop...")
	}()

	ctx := tool.NewCtxBK()

	for range time.Tick(5 * time.Second) {
		s.flushQueueLenExp(ctx)

		delayQLen, _ := s.redis.ZCard(ctx, s.key).Uint64()
		s.exp.Gauge("delay_queue_length").Set(float64(delayQLen))

		redisPing, err := s.redis.Ping(ctx).Result()
		if err != nil {
			s.exp.Gauge("redis_ping").Set(0)
		}
		if redisPing == "PONG" {
			s.exp.Gauge("redis_ping").Set(1)
		}
	}
}

// flushQueueLenExp 按已 LaunchJob 的 key 打点 bufferQ / unAck 队列长度.
func (s *Svc) flushQueueLenExp(ctx context.Context) {
	s.jobKeys.Range(func(k, _ any) bool {
		key := k.(string)
		labels := map[string]string{"key": key}

		if n, err := s.redis.LLen(ctx, s.bufferQName(key)).Result(); err == nil {
			s.exp.GaugeWith("buffer_queue_length", labels).Set(float64(n))
		}
		if n, err := s.redis.ZCard(ctx, s.unAckQName(key)).Result(); err == nil {
			s.exp.GaugeWith("unAck_queue_length", labels).Set(float64(n))
		}

		return true
	})
}
