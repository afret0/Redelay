package delayTask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/afret0/wheel/tool"
	"github.com/bsm/redislock"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func (s *Service) startTick() {
	logger := GetLogger()
	defer func() { logger.Infof("tick stop...") }()

	for range time.Tick(1 * time.Second) {
		if s.debug {
			logger.Infof("tick..")
		}
		go s.tickQ()
		go s.tickUnAckQ()
	}
}

func (s *Service) tickQ() {
	lg := GetLogger()

	count := int64(50)
	envCountS := os.Getenv("DELAYTASK_TICK_COUNT")
	envCount := tool.ConStringToInt64WithoutErr(envCountS)
	if envCount != 0 {
		count = envCount
	}
	lg.Infof("count: %d, envCountS: %s, envCount: %d", count, envCountS, envCount)

	now := time.Now().Unix()
	eventL, err := s.redis.ZRangeByScore(context.Background(), s.key, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now), Count: count}).Result()
	if err != nil {
		logger.Errorf("get event failed: %v", err)
		return
	}

	for _, v := range eventL {
		ctx := context.Background()
		ctx = context.WithValue(ctx, "opId", strings.ReplaceAll(uuid.New().String(), "-", ""))
		//lg := CtxLogger(ctx)

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

		go s.handleEvent(ctx, v)

	}
}

func (s *Service) tickUnAckQ() {
	eventL, err := s.redis.ZRangeByScore(context.Background(), s.UnAckKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", time.Now().Add(-3*time.Minute).Unix())}).Result()
	//eventL, err := s.redis.ZRangeByScore(context.Background(), s.UnAckKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", time.Now().Add(-10*time.Second).Unix())}).Result()
	if err != nil {
		logger.Errorf("get event failed: %v", err)
		return
	}

	for _, v := range eventL {
		ctx := context.Background()
		ctx = context.WithValue(ctx, "opId", strings.ReplaceAll(uuid.New().String(), "-", ""))

		v := v

		e := new(event)
		err := json.Unmarshal([]byte(v), e)
		if err != nil {
			logger.Errorf("unmarshal event failed: %v", err)
			continue
		}
		if e.UnAckRetryCount >= 3 {
			logger.Errorf("event: %s retry 3 times, discard", e.Id)
			s.redis.ZRem(ctx, s.UnAckKey, v)
			continue
		}

		e.UnAckRetryCount += 1
		eB, err := json.Marshal(e)
		if err != nil {
			logger.Errorf("marshal event failed: %v", err)
			continue
		}

		p := s.redis.Pipeline()
		p.ZRem(ctx, s.UnAckKey, v)
		p.ZAdd(ctx, s.key, redis.Z{Score: float64(time.Now().Unix()), Member: string(eB)})
		_, err = p.Exec(ctx)
		if err != nil {
			logger.Errorf("tickUnAckQ failed: %v", err)
			continue
		}
	}
}

func (s *Service) handleEvent(ctx context.Context, eventS string) {
	lg := CtxLogger(ctx).WithField("event", eventS)
	pipe := s.redis.Pipeline()

	pipe.ZRem(ctx, s.key, eventS)
	pipe.ZAdd(ctx, s.UnAckKey, redis.Z{Score: float64(time.Now().Unix()), Member: eventS})
	_, err := pipe.Exec(ctx)
	if err != nil {
		lg.Errorf("handle event failed: %v", err)
		return
	}

	err = s.runEvent(ctx, eventS)
	if err != nil {
		if errors.Is(err, RetryErr) {
			e := new(event)
			err := json.Unmarshal([]byte(eventS), e)
			if err != nil {
				lg.Errorf("unmarshal event failed: %v", err)
				return
			}
			if e.RetryCount >= 3 {
				s.redis.ZRem(ctx, s.UnAckKey, eventS)
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
	pipe1.ZRem(ctx, s.key, eventS)
	pipe1.ZRem(ctx, s.UnAckKey, eventS)
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
	lg := CtxLogger(ctx).WithField("event", eventS)
	lg.Infof("event run...")

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

	lg.Infof("event end...")
	return nil
}
