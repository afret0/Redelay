package Redelay

import (
	"context"
	"time"

	"github.com/afret0/wheel/log"
	"github.com/afret0/wheel/tool"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

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

	go s.startTick()

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

	for {
		eventSL, err := s.redis.BLPop(ctx, 0, s.bufferQName(key)).Result()
		if err != nil {
			lg.Errorf("get event failed: %s", err)
			continue
		}

		err = s.redis.RPush(ctx, s.bufferQName(key), eventSL[1]).Err()
		if err != nil {
			lg.Errorf("err: %d", err)
		}

		if err := limiter.Wait(ctx); err != nil {
			lg.Errorf("rate limit wait failed: %s", err)
			continue
		}

		s.exp.CounterWith("consume_total", jobLabels).Inc()

		go s.handleEvent(ctx, eventSL[1], f)
		//err = s.antsPool.Submit(func() {
		//	s.handleEvent(ctx, eventSL[1])
		//})
		//if err != nil {
		//	lg.Errorf("submit handleEvent to ants pool failed: %s", err)
		//}

	}

}
