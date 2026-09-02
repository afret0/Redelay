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

func (s *Svc) startTick() {
	GetLogger().Infof("start tick")
	s.exp.Gauge("tick_ping").Set(1)
	defer func() {
		s.tickRunTag = 0
		s.exp.Gauge("tick_ping").Set(0)
		GetLogger().Infof("tick stop...")
	}()

	if s.tickRunTag == 1 {
		GetLogger().Infof("tick is running, skip")
		return
	}

	s.tickRunTag = 1

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

		//v := v

		LT := 10 * time.Minute
		_, err = s.lock.Obtain(ctx, fmt.Sprintf("%s:Redelay:tickQ:lock:%s", s.svcName, tool.MD5(v)), LT, nil)
		if err != nil {
			if errors.Is(err, redislock.ErrNotObtained) {
				return
			}
			//GetLogger().Errorf("obtain lock failed, 不执行, err: %s", err)
			return
		}

		E := &event{}
		err = tool.Unmarshal(v, E)
		if err != nil {
			GetLogger().Errorf("unmarshal event: %s, failed: %s", v, err)
			return
		}

		pipe := s.redis.Pipeline()

		pipe.RPush(ctx, s.bufferQName(E.Key), v)
		pipe.ZRem(ctx, s.key, v)
		//pipe.ZAdd(ctx, fmt.Sprintf("%s:%s", s.unAckKey, E.Key), redis.Z{Score: float64(time.Now().Unix()), Member: v})

		_, err := pipe.Exec(ctx)
		if err != nil {
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

//func (s *Svc) tickUnAckQ() {
//
//	eventL, err := s.redis.ZRangeByScore(context.Background(), s.unAckKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", time.Now().Add(-3*time.Minute).Unix()), Count: s.tickQCount}).Result()
//	//eventL, err := s.redis.ZRangeByScore(context.Background(), s.UnAckKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", time.Now().Add(-10*time.Second).Unix())}).Result()
//	if err != nil {
//		logger.Errorf("get event failed: %v", err)
//		return
//	}
//
//	for _, v := range eventL {
//		ctx := context.Background()
//		ctx = context.WithValue(ctx, "opId", strings.ReplaceAll(uuid.New().String(), "-", ""))
//
//		s.exp.Counter("tickUnAckQ_tick_total").Inc()
//
//		//v := v
//
//		e := new(event)
//		err := json.Unmarshal([]byte(v), e)
//		if err != nil {
//			logger.Errorf("unmarshal event failed: %v", err)
//			continue
//		}
//		if e.UnAckRetryCount >= 3 {
//			logger.Errorf("event: %s retry 3 times, discard", e.Id)
//			s.redis.ZRem(ctx, s.unAckKey, v)
//			continue
//		}
//
//		e.UnAckRetryCount += 1
//		eB, err := json.Marshal(e)
//		if err != nil {
//			logger.Errorf("marshal event failed: %v", err)
//			continue
//		}
//
//		p := s.redis.Pipeline()
//		p.ZRem(ctx, s.unAckKey, v)
//		p.ZAdd(ctx, s.key, redis.Z{Score: float64(time.Now().Unix()), Member: string(eB)})
//		_, err = p.Exec(ctx)
//		if err != nil {
//			logger.Errorf("tickUnAckQ failed: %v", err)
//			continue
//		}
//	}
//}

//func (s *Svc) startConsume() {
//	s.exp.Gauge("consume_ping").Set(1)
//	defer func() {
//		s.exp.Gauge("consume_ping").Set(0)
//		GetLogger().Infof("consume stop...")
//	}()
//
//	GetLogger().Infof("consume start...")
//	ctx := tool.NewCtxBK()
//
//	limiter := rate.NewLimiter(rate.Limit(s.consumeLimit), int(s.consumeLimit*2))
//
//	for {
//		eventSL, err := s.redis.BLPop(ctx, 0, s.bufferQueue).Result()
//		if err != nil {
//			GetLogger().Errorf("get event failed: %s", err)
//			continue
//		}
//
//		if err := limiter.Wait(ctx); err != nil {
//			GetLogger().Errorf("rate limit wait failed: %s", err)
//			continue
//		}
//
//		s.exp.Counter("consume_total").Inc()
//
//		go s.handleEvent(ctx, eventSL[1])
//		//err = s.antsPool.Submit(func() {
//		//	s.handleEvent(ctx, eventSL[1])
//		//})
//		//if err != nil {
//		//	GetLogger().Errorf("submit handleEvent to ants pool failed: %s", err)
//		//}
//
//	}
//
//}

func (s *Svc) handleEvent(ctx context.Context, eventS string, f func(p string) error) {

	err := s.runEvent(ctx, eventS, f)
	if err != nil {
		if errors.Is(err, RetryErr) {
			e := new(event)
			err := json.Unmarshal([]byte(eventS), e)
			if err != nil {
				GetLogger().Errorf("unmarshal event failed: %v", err)
				return
			}
			if e.RetryCount >= 3 {
				s.redis.ZRem(ctx, s.unAckBufferQPrefix, eventS)
				GetLogger().Errorf("event: %s retry 3 times, discard", e.Id)
				return
			}
			e.RetryCount += 1
			eB, err := json.Marshal(e)
			if err != nil {
				GetLogger().Errorf("marshal event failed: %v", err)
				return
			}
			s.redis.ZAdd(ctx, s.key, redis.Z{Score: float64(time.Now().Unix() + 3*e.RetryCount), Member: string(eB)})
		}
		GetLogger().Errorf("run event failed: %v", err)
		return
	}

	pipe1 := s.redis.Pipeline()
	//pipe1.ZRem(ctx, s.key, eventS)
	pipe1.ZRem(ctx, s.unAckBufferQPrefix, eventS)
	_, err = pipe1.Exec(ctx)
	if err != nil {
		GetLogger().Errorf("handle event failed: %v", err)
		return
	}
}

func (s *Svc) runEvent(ctx context.Context, eventS string, f func(p string) error) error {
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("run event panic: %v", err)
		}
	}()
	if s.Debug() {
		GetLogger().Infof("event run, eventS: %s", eventS)
	}

	E := new(event)
	err := json.Unmarshal([]byte(eventS), E)
	if err != nil {
		GetLogger().WithError(err).Errorf("unmarshal event failed, event: %s", eventS)
		return err
	}

	//s.mx.RLock()
	//f, ok := s.slot[E.Name]
	//s.mx.RUnlock()
	//
	//if !ok {
	//	err := fmt.Errorf("event: %s func is unregister", E.Key)
	//	GetLogger().Error(err)
	//	return err
	//}

	LT := 10 * time.Minute
	_, err = s.lock.Obtain(ctx, fmt.Sprintf("%s:Redelay:runEvent:lock:%s", s.svcName, tool.MD5(eventS)), LT, nil)
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			GetLogger().Infof("未获取到锁, 判断为重复执行")
			return err
		}
		GetLogger().Errorf("obtain lock failed, 不执行, err: %s", err)
		return err
	}
	//defer func() { _ = lk.Release(ctx) }()

	err = f(E.Args)
	if err != nil {
		GetLogger().Errorf("run event failed, err: %v", err)
		return err
	}

	if s.Debug() {
		GetLogger().Infof("event end, eventS: %s", eventS)
	}
	return nil
}
