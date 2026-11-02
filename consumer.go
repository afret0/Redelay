package Redelay

import (
	"context"
	"errors"
	"sync"
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
	timeout := jobTimeoutFromEnv()

	return func(msgS string) error {
		c1, cancel := context.WithTimeout(context.Background(), timeout)
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

func (s *Svc) LaunchJob(ctx context.Context, key string, f Job) {

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
	lg := log.CtxLogger(ctx).WithFields(logrus.Fields{"key": key})

	// drain: ctx 取消后只停止认领新事件, 等在飞的 handler 自然跑完再返回.
	// 在飞任务必须用脱离取消的 ctx, 否则它的 ack 会因 context canceled 失败,
	// 事件留在 unAck 被扫描器重投, 同一条业务被执行两次.
	var inFlight sync.WaitGroup

	limiter := rate.NewLimiter(rate.Limit(s.consumeLimit), int(s.consumeLimit*2))

	idle := time.Duration(s.tickInterval) * time.Millisecond

consumeLoop:
	for {
		// 先限流再认领, 避免消息被摘到 unAck 后长时间排队, 被扫描器误判为超时重投.
		// ctx 取消后 limiter.Wait 会立刻返回错误, 这里必须先判取消再退出, 否则空转刷日志.
		if err := limiter.Wait(ctx); err != nil {
			if ctx.Err() != nil {
				lg.Infof("done, limiter wait canceled, consume loop exit, err: %s", ctx.Err())
				break consumeLoop
			}
			lg.Errorf("rate limit wait failed: %s", err)
			continue
		}

		// 先拿并发配额再认领: 满了就阻塞在这里, 事件不会被摘到 unAck 里空等.
		eventS, err := s.claimEvent(ctx, key)
		if err != nil {
			if ctx.Err() != nil {
				lg.Infof("done, claim canceled, consume loop exit, err: %s", ctx.Err())
				break consumeLoop
			}

			if errors.Is(err, redis.Nil) {
				select {
				case <-ctx.Done():
					lg.Infof("done, idle wait canceled, consume loop exit, err: %s", ctx.Err())
					break consumeLoop
				case <-time.After(idle):
				}
				continue
			}

			lg.Errorf("fail, get event failed, err: %s", err)
			select {
			case <-ctx.Done():
				lg.Infof("done, claim failed then canceled, consume loop exit, err: %s", ctx.Err())
				break consumeLoop
			case <-time.After(idle):
			}
			continue
		}

		s.exp.CounterWith("consume_total", jobLabels).Inc()

		inFlight.Add(1)
		go func(eventS string) {
			defer inFlight.Done()
			defer func() { <-s.slots }()
			s.handleEvent(tool.DetachCtx(ctx, tool.OpId(ctx)), eventS, f)
		}(eventS)
	}

	inFlight.Wait()
	lg.Infof("done, in-flight drained, consume loop exit")
}

// claimEvent 占一个并发配额后原子认领队头事件, 认领失败立即归还配额,
// 成功则把配额交给调用方, 由 handler goroutine 结束时归还.
func (s *Svc) claimEvent(ctx context.Context, key string) (string, error) {
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	eventS, err := lpopZAddUnAckScript.Run(ctx, s.redis,
		[]string{s.bufferQName(key), s.unAckQName(key)},
		timeTool.Now().Unix(),
	).Text()
	if err != nil {
		<-s.slots
		return "", err
	}

	return eventS, nil
}
