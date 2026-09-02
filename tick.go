package Redelay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/afret0/wheel/tool/timeTool"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

// errDuplicateExec 表示同一条消息正被其他实例执行, 本次跳过.
var errDuplicateExec = errors.New("duplicate exec")

// zremRPushScript 原子地把事件从延时 ZSET(KEYS[1]) 移到 bufferQ(KEYS[2]).
// ZREM 返回 0 说明已被其他实例取走, 不再重复投递.
var zremRPushScript = redis.NewScript(`
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
	return 0
end
redis.call('RPUSH', KEYS[2], ARGV[1])
return 1
`)

// zremZAddScript 原子地把事件从 unAck ZSET(KEYS[1]) 摘除, 并重新投递回延时 ZSET(KEYS[2]).
// ZREM 返回 0 说明已被其他实例处理, 不再重复投递.
var zremZAddScript = redis.NewScript(`
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then
	return 0
end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
return 1
`)

func (s *Svc) startTick() {
	release, ok := s.tryRun("tickQ")
	if !ok && s.Debug() {
		GetLogger().Infof("tick is running, skip")
		return
	}
	defer release()

	if s.Debug() {
		GetLogger().Infof("start tick")
	}

	s.exp.Gauge("tick_ping").Set(1)
	defer func() {
		s.exp.Gauge("tick_ping").Set(0)
		GetLogger().Infof("tick stop...")
	}()

	dur := time.Duration(s.tickInterval) * time.Millisecond
	for range time.Tick(dur) {
		if s.Debug() {
			GetLogger().Infof("tick...")
		}

		al, err := redis_rate.NewLimiter(s.redis).AllowN(context.Background(), fmt.Sprintf("%s:tickQ:tick:lock", s.svcName), redis_rate.Limit{Rate: 1, Burst: 1, Period: dur}, 1)
		if err != nil {
			GetLogger().Errorf("err: %s", err)
			continue
		}
		if al.Allowed <= 0 {
			if s.Debug() {
				GetLogger().Infof("another tick is running, skip")
			}
			continue
		}

		s.tickQ()
		//err := s.antsPool.Submit(s.tickQ)
		//if err != nil {
		//	GetLogger().Errorf("submit tickQ to ants pool failed: %v", err)
		//	continue
		//}

		//go s.tickUnAckQ()
		//err = s.antsPool.Submit(s.tickUnAckQ)
		//if err != nil {
		//	GetLogger().Errorf("submit tickUnAckQ to ants pool failed: %v", err)
		//	continue
		//}

	}

}

func (s *Svc) tickQ() {

	now := timeTool.Now().Unix()
	eventL, err := s.redis.ZRangeByScore(context.Background(), s.key, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now), Count: s.tickQCount}).Result()
	if err != nil {
		logger.Errorf("get event failed: %v", err)
		return
	}

	for _, v := range eventL {
		ctx := context.Background()
		ctx = context.WithValue(ctx, "opId", tool.UUIDWithoutHyphen())

		s.exp.Counter("tickQ_tick_total").Inc()

		E := &event{}
		err = tool.Unmarshal(v, E)
		if err != nil {
			GetLogger().Errorf("unmarshal event: %s, failed: %s", v, err)
			continue
		}

		if err := zremRPushScript.Run(ctx, s.redis, []string{s.key, s.bufferQName(E.Key)}, v).Err(); err != nil {
			GetLogger().Errorf("handle event failed: %v", err)
			return
		}

		//go s.handleEvent(ctx, v)

	}
}

func (s *Svc) bufferQName(key string) string {
	return fmt.Sprintf("%s:%s", s.bufferQPrefix, key)
}

func (s *Svc) unAckQName(key string) string {
	return fmt.Sprintf("%s:%s", s.unAckBufferQPrefix, key)
}

// startTickUnAckQ 周期性扫描某个 key 的 unAck ZSET, 把认领后长时间未 ack 的事件
// (消费者崩溃/被 k8s 驱逐导致) 重新投递回延时 ZSET.
func (s *Svc) startTickUnAckQ(key string) {
	release, ok := s.tryRun("tickUnAckQ:" + key)
	if !ok && s.Debug() {
		GetLogger().Infof("tickUnAckQ is running, skip, key: %s", key)
		return
	}
	defer release()

	jobLabels := map[string]string{"key": key}

	if s.Debug() {
		GetLogger().Infof("start tickUnAckQ, key: %s", key)
	}
	s.exp.GaugeWith("tick_unack_ping", jobLabels).Set(1)
	defer func() {
		s.exp.GaugeWith("tick_unack_ping", jobLabels).Set(0)
		GetLogger().Infof("tickUnAckQ stop, key: %s", key)
	}()

	dur := s.unAckTimeout
	for range time.Tick(dur) {
		al, err := redis_rate.NewLimiter(s.redis).AllowN(context.Background(), fmt.Sprintf("%s:tickUnAckQ:tick:lock:%s", s.svcName, key), redis_rate.Limit{Rate: 1, Burst: 1, Period: dur}, 1)
		if err != nil {
			GetLogger().Errorf("err: %s", err)
			continue
		}
		if al.Allowed <= 0 {
			if s.Debug() {
				GetLogger().Infof("another tickUnAckQ is running, skip")
			}
			continue
		}

		s.tickUnAckQ(key)
	}
}

func (s *Svc) tickUnAckQ(key string) {
	unAckQ := s.unAckQName(key)
	deadline := timeTool.Now().Add(-s.unAckTimeout).Unix()

	eventL, err := s.redis.ZRangeByScore(context.Background(), unAckQ, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", deadline), Count: s.tickQCount}).Result()
	if err != nil {
		GetLogger().Errorf("get unAck event failed: %v", err)
		return
	}

	for _, v := range eventL {
		ctx := context.Background()
		ctx = context.WithValue(ctx, "opId", tool.UUIDWithoutHyphen())

		s.exp.CounterWith("tickUnAckQ_tick_total", map[string]string{"key": key}).Inc()

		e := new(event)
		if err := json.Unmarshal([]byte(v), e); err != nil {
			GetLogger().Errorf("unmarshal event: %s, failed: %s", v, err)
			continue
		}

		if e.UnAckRetryCount >= 3 {
			s.redis.ZRem(ctx, unAckQ, v)
			GetLogger().Errorf("event: %s unAck retry 3 times, discard", e.Id)
			continue
		}

		e.UnAckRetryCount += 1
		eB, err := json.Marshal(e)
		if err != nil {
			GetLogger().Errorf("marshal event failed: %v", err)
			continue
		}

		if err := s.requeue(ctx, unAckQ, v, string(eB), timeTool.Now().Unix()); err != nil {
			GetLogger().Errorf("tickUnAckQ requeue failed: %v", err)
			continue
		}
	}
}

func (s *Svc) handleEvent(ctx context.Context, eventS string, f func(p string) error) {

	e := new(event)
	if err := json.Unmarshal([]byte(eventS), e); err != nil {
		GetLogger().Errorf("unmarshal event failed: %v", err)
		return
	}

	unAckQ := s.unAckQName(e.Key)

	// 硬超时闸门: 超过 jobTimeout 视为执行成功, 直接 ack 不再重试,
	// 避免任务卡死后被 unAck 扫描器无限重投.
	// 注意 Job 签名不带 ctx, 超时后无法真正取消, 其 goroutine 会继续跑到自然结束.
	done := make(chan error, 1)
	go func() {
		done <- s.runEvent(ctx, eventS, f)
	}()

	var err error
	timer := time.NewTimer(s.jobTimeout)
	defer timer.Stop()

	select {
	case err = <-done:
	case <-timer.C:
		s.exp.CounterWith("job_timeout_total", map[string]string{"key": e.Key}).Inc()
		GetLogger().Errorf("event: %s exec timeout(%s), ack anyway", e.Id, s.jobTimeout)
		if err := s.redis.ZRem(ctx, unAckQ, eventS).Err(); err != nil {
			GetLogger().Errorf("ack timeout event failed: %v", err)
		}
		return
	}

	if err != nil {
		// 同一条消息正被其他实例执行, 不 ack 也不重投, 交给持锁方处理.
		if errors.Is(err, errDuplicateExec) {
			s.exp.CounterWith("duplicate_exec_total", map[string]string{"key": e.Key}).Inc()
			return
		}

		if errors.Is(err, RetryErr) {
			if e.RetryCount >= 3 {
				s.redis.ZRem(ctx, unAckQ, eventS)
				GetLogger().Errorf("event: %s retry 3 times, discard", e.Id)
				return
			}

			e.RetryCount += 1
			eB, err := json.Marshal(e)
			if err != nil {
				GetLogger().Errorf("marshal event failed: %v", err)
				return
			}

			if err := s.requeue(ctx, unAckQ, eventS, string(eB), time.Now().Unix()+3*e.RetryCount); err != nil {
				GetLogger().Errorf("retry event failed: %v", err)
			}
			return
		}

		// 业务错误不重试, 直接 ack, 避免消息被反复投递消费.
		s.exp.CounterWith("job_failed_total", map[string]string{"key": e.Key}).Inc()
		GetLogger().Errorf("run event failed, ack anyway: %v", err)
		if err := s.redis.ZRem(ctx, unAckQ, eventS).Err(); err != nil {
			GetLogger().Errorf("ack failed event failed: %v", err)
		}
		return
	}

	if err := s.redis.ZRem(ctx, unAckQ, eventS).Err(); err != nil {
		GetLogger().Errorf("handle event failed: %v", err)
		return
	}
}

// requeue 原子地把事件从 unAck ZSET 摘除并按 score 重新投递回延时 ZSET.
func (s *Svc) requeue(ctx context.Context, unAckQ, oldEventS, newEventS string, score int64) error {
	return zremZAddScript.Run(ctx, s.redis,
		[]string{unAckQ, s.key},
		oldEventS, newEventS, score,
	).Err()
}

func (s *Svc) runEvent(ctx context.Context, eventS string, f func(p string) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// panic 视为业务失败, 由调用方 ack, 避免同一条消息反复投递.
			err = fmt.Errorf("run event panic: %v", r)
			GetLogger().Error(err)
		}
	}()
	if s.Debug() {
		GetLogger().Infof("event run, eventS: %s", eventS)
	}

	E := new(event)
	if err = json.Unmarshal([]byte(eventS), E); err != nil {
		GetLogger().WithError(err).Errorf("unmarshal event failed, event: %s", eventS)
		return err
	}

	// 按事件 Id 加锁: Id 在重试/重投之间保持不变, 保证同一条消息不会被多个进程同时消费.
	// TTL 与任务硬超时对齐, 持有者崩溃后最多 jobTimeout 就能被重新消费.
	lk, err := s.lock.Obtain(ctx, fmt.Sprintf("%s:Redelay:runEvent:lock:%s", s.svcName, E.Id), s.jobTimeout, nil)
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			GetLogger().Infof("event: %s 正在其他实例执行, 跳过", E.Id)
			return errDuplicateExec
		}
		GetLogger().Errorf("obtain lock failed, 不执行, err: %s", err)
		return err
	}
	// 必须释放, 否则 Id 相同的重试消息会被自己上一轮的锁挡住.
	defer func() { _ = lk.Release(ctx) }()

	if err = f(E.Args); err != nil {
		GetLogger().Errorf("run event failed, err: %v", err)
		return err
	}

	if s.Debug() {
		GetLogger().Infof("event end, eventS: %s", eventS)
	}
	return nil
}
