package Redelay

import (
	"context"
	"errors"
	"time"

	"github.com/afret0/wheel/log"
	"github.com/afret0/wheel/tool"
	"github.com/afret0/wheel/tool/timeTool"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// lpopZAddUnAckScript 原子地把 bufferQ(KEYS[1]) 队头元素认领到 unAck ZSET(KEYS[2]),
// score 为认领时间(ARGV[1]), 供兜底扫描器判断滞留时长. 队列为空时返回 nil.
var lpopZAddUnAckScript = redis.NewScript(`
local v = redis.call('LPOP', KEYS[1])
if not v then
	return nil
end
redis.call('ZADD', KEYS[2], ARGV[1], v)
return v
`)

type Job func(p string) error

func NewJob[T any](f func(ctx context.Context, p T) error) Job {
	return func(msgS string) error {
		c1, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		lg1 := log.CtxLogger(c1).WithFields(logrus.Fields{})
		if tool.Debug() {
			lg1.Printf("receive msg: %s", string(msgS))
		}

		M := &Message[T]{}
		err := tool.Unmarshal(msgS, M)
		if err != nil {
			lg1.Printf("unmarshal message error: %s, msg: %s", err, msgS)
			return err
		}

		ctx := context.WithValue(c1, "opId", M.MsgId)
		lg := log.CtxLogger(ctx).WithFields(logrus.Fields{})
		if tool.Debug() {
			lg.Infof("start process message: %s", msgS)
		}

		err = f(ctx, M.Data)
		if err != nil {
			lg.Errorf("process message error: %s", err)
			return err
		}

		if tool.Debug() {
			lg.Infof("process message success: %s", msgS)
		}

		return nil
	}
}

func (s *Svc) LaunchJob(key string, f Job) error {

	s.jobKeys.Store(key, struct{}{})

	go s.startTick()
	go s.startTickUnAckQ(key)
	go s.loopFlushExp()

	jobLabels := map[string]string{"key": key}

	s.exp.GaugeWith("consume_ping", jobLabels).Set(1)
	defer func() {
		s.exp.GaugeWith("consume_ping", jobLabels).Set(0)
		GetLogger().Infof("consume stop...")
	}()

	log.GetLogger().Infof("consume start...")
	ctx := tool.NewCtxBK()
	lg := log.CtxLogger(ctx).WithFields(logrus.Fields{"key": key})

	limiter := rate.NewLimiter(rate.Limit(s.consumeLimit), int(s.consumeLimit*2))

	idle := time.Duration(s.tickInterval) * time.Millisecond

	for {
		// 先限流再认领, 避免消息被摘到 unAck 后长时间排队, 被扫描器误判为超时重投.
		if err := limiter.Wait(ctx); err != nil {
			lg.Errorf("rate limit wait failed: %s", err)
			continue
		}

		eventS, err := lpopZAddUnAckScript.Run(ctx, s.redis,
			[]string{s.bufferQName(key), s.unAckQName(key)},
			timeTool.Now().Unix(),
		).Text()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(idle):
				}
				continue
			}
			lg.Errorf("get event failed: %s", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(idle):
			}
			continue
		}

		s.exp.CounterWith("consume_total", jobLabels).Inc()

		go s.handleEvent(ctx, eventS, f)
		//err = s.antsPool.Submit(func() {
		//	s.handleEvent(ctx, eventSL[1])
		//})
		//if err != nil {
		//	lg.Errorf("submit handleEvent to ants pool failed: %s", err)
		//}

	}

}
