package delayTask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/afret0/wheel/tool/timeTool"
	"github.com/bsm/redislock"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

func (s *Service) startTick() {
	lg.Infof("start tick")
	s.exp.Gauge("tick_ping").Set(1)
	defer func() {
		s.exp.Gauge("tick_ping").Set(0)
		lg.Infof("tick stop...")
	}()

	for range time.Tick(time.Duration(s.tickInterval) * time.Millisecond) {
		if s.Debug() {
			lg.Infof("tick...")
		}

		go s.tickQ()
		//err := s.antsPool.Submit(s.tickQ)
		//if err != nil {
		//	lg.Errorf("submit tickQ to ants pool failed: %v", err)
		//	continue
		//}

		go s.tickUnAckQ()
		//err = s.antsPool.Submit(s.tickUnAckQ)
		//if err != nil {
		//	lg.Errorf("submit tickUnAckQ to ants pool failed: %v", err)
		//	continue
		//}

	}

}

func (s *Service) tickQ() {

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

		v := v

		LT := 10 * time.Minute
		_, err = s.lock.Obtain(ctx, fmt.Sprintf("%s:delayTask:tickQ:lock:%s", s.caller, tool.MD5(v)), LT, nil)
		if err != nil {
			if errors.Is(err, redislock.ErrNotObtained) {
				return
			}
			//lg.Errorf("obtain lock failed, 不执行, err: %s", err)
			return
		}

		pipe := s.redis.Pipeline()

		pipe.RPush(ctx, s.bufferQueue, v)
		pipe.ZRem(ctx, s.key, v)
		pipe.ZAdd(ctx, s.unAckKey, redis.Z{Score: float64(time.Now().Unix()), Member: v})
		_, err := pipe.Exec(ctx)
		if err != nil {
			lg.Errorf("handle event failed: %v", err)
			return
		}

		//go s.handleEvent(ctx, v)

	}
}

func (s *Service) tickUnAckQ() {

	eventL, err := s.redis.ZRangeByScore(context.Background(), s.unAckKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", time.Now().Add(-3*time.Minute).Unix()), Count: s.tickQCount}).Result()
	//eventL, err := s.redis.ZRangeByScore(context.Background(), s.UnAckKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", time.Now().Add(-10*time.Second).Unix())}).Result()
	if err != nil {
		logger.Errorf("get event failed: %v", err)
		return
	}

	for _, v := range eventL {
		ctx := context.Background()
		ctx = context.WithValue(ctx, "opId", strings.ReplaceAll(uuid.New().String(), "-", ""))

		s.exp.Counter("tickUnAckQ_tick_total").Inc()

		v := v

		e := new(event)
		err := json.Unmarshal([]byte(v), e)
		if err != nil {
			logger.Errorf("unmarshal event failed: %v", err)
			continue
		}
		if e.UnAckRetryCount >= 3 {
			logger.Errorf("event: %s retry 3 times, discard", e.Id)
			s.redis.ZRem(ctx, s.unAckKey, v)
			continue
		}

		e.UnAckRetryCount += 1
		eB, err := json.Marshal(e)
		if err != nil {
			logger.Errorf("marshal event failed: %v", err)
			continue
		}

		p := s.redis.Pipeline()
		p.ZRem(ctx, s.unAckKey, v)
		p.ZAdd(ctx, s.key, redis.Z{Score: float64(time.Now().Unix()), Member: string(eB)})
		_, err = p.Exec(ctx)
		if err != nil {
			logger.Errorf("tickUnAckQ failed: %v", err)
			continue
		}
	}
}

func (s *Service) startConsume() {
	s.exp.Gauge("consume_ping").Set(1)
	defer func() {
		s.exp.Gauge("consume_ping").Set(0)
		lg.Infof("consume stop...")
	}()

	lg.Infof("consume start...")
	ctx := tool.NewCtxBK()

	limiter := rate.NewLimiter(rate.Limit(s.consumeLimit), int(s.consumeLimit*2))

	for {
		eventSL, err := s.redis.BLPop(ctx, 0, s.bufferQueue).Result()
		if err != nil {
			lg.Errorf("get event failed: %s", err)
			continue
		}

		if err := limiter.Wait(ctx); err != nil {
			lg.Errorf("rate limit wait failed: %s", err)
			continue
		}

		s.exp.Counter("consume_total").Inc()

		go s.handleEvent(ctx, eventSL[1])
		//err = s.antsPool.Submit(func() {
		//	s.handleEvent(ctx, eventSL[1])
		//})
		//if err != nil {
		//	lg.Errorf("submit handleEvent to ants pool failed: %s", err)
		//}

	}

}

func (s *Service) handleEvent(ctx context.Context, eventS string) {

	err := s.runEvent(ctx, eventS)
	if err != nil {
		if errors.Is(err, RetryErr) {
			e := new(event)
			err := json.Unmarshal([]byte(eventS), e)
			if err != nil {
				lg.Errorf("unmarshal event failed: %v", err)
				return
			}
			if e.RetryCount >= 3 {
				s.redis.ZRem(ctx, s.unAckKey, eventS)
				lg.Errorf("event: %s retry 3 times, discard", e.Id)
				return
			}
			e.RetryCount += 1
			eB, err := json.Marshal(e)
			if err != nil {
				lg.Errorf("marshal event failed: %v", err)
				return
			}
			s.redis.ZAdd(ctx, s.key, redis.Z{Score: float64(time.Now().Unix() + 3*e.RetryCount), Member: string(eB)})
		}
		lg.Errorf("run event failed: %v", err)
		return
	}

	pipe1 := s.redis.Pipeline()
	//pipe1.ZRem(ctx, s.key, eventS)
	pipe1.ZRem(ctx, s.unAckKey, eventS)
	_, err = pipe1.Exec(ctx)
	if err != nil {
		lg.Errorf("handle event failed: %v", err)
		return
	}
}

func (s *Service) runEvent(ctx context.Context, eventS string) error {
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("run event panic: %v", err)
		}
	}()
	if s.Debug() {
		lg.Infof("event run, eventS: %s", eventS)
	}

	E := new(event)
	err := json.Unmarshal([]byte(eventS), E)
	if err != nil {
		lg.WithError(err).Errorf("unmarshal event failed, event: %s", eventS)
		return err
	}

	s.mx.RLock()
	f, ok := s.slot[E.Name]
	s.mx.RUnlock()

	if !ok {
		err := fmt.Errorf("event: %s func is unregister", E.Name)
		lg.Error(err)
		return err
	}

	LT := 10 * time.Minute
	_, err = s.lock.Obtain(ctx, fmt.Sprintf("%s:delayTask:runEvent:lock:%s", s.caller, tool.MD5(eventS)), LT, nil)
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			lg.Infof("未获取到锁, 判断为重复执行")
			return err
		}
		lg.Errorf("obtain lock failed, 不执行, err: %s", err)
		return err
	}
	//defer func() { _ = lk.Release(ctx) }()

	err = f(E.Args)
	if err != nil {
		lg.Errorf("run event failed, err: %v", err)
		return err
	}

	if s.Debug() {
		lg.Infof("event end, eventS: %s", eventS)
	}
	return nil
}
