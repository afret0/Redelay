package delayTask

import (
	"fmt"
	"os"
	"sync"

	"github.com/afret0/wheel/tool"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

var RetryErr = fmt.Errorf("retry")

type Service struct {
	redis    redis.UniversalClient
	caller   string
	slot     map[string]func(p string) error
	mx       sync.RWMutex
	key      string
	unAckKey string
	init     bool
	lock     *redislock.Client
	debug    bool

	tickQCount   int64
	tickInterval int64
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

	lg := GetLogger()

	count := int64(50)
	envCountS := os.Getenv("DELAYTASK_TICK_COUNT")
	envCount := tool.ConStringToInt64WithoutErr(envCountS)
	if envCount != 0 {
		count = envCount
	}

	tickInterval := int64(300)
	envTickIntervalS := os.Getenv("DELAYTASK_TICK_INTERVAL_MS")
	envTickInterval := tool.ConStringToInt64WithoutErr(envTickIntervalS)
	if envTickInterval != 0 {
		tickInterval = envTickInterval
	}

	lg.Infof("count: %d, envCountS: %s, envCount: %d", count, envCountS, envCount)

	svr := &Service{
		redis:    redis,
		caller:   caller,
		slot:     make(map[string]func(p string) error),
		mx:       sync.RWMutex{},
		key:      fmt.Sprintf("%s:delayTask", caller),
		unAckKey: fmt.Sprintf("%s:delayTask:unAck", caller),
		init:     true,
		lock:     redislock.New(redis),

		tickQCount:   count,
		tickInterval: tickInterval,
	}

	go svr.startTick()

	return svr
}

func (s *Service) SetDebug(debug bool) {
	s.debug = debug
}
