package Redelay

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func (s *Svc) Publish(ctx context.Context, key string, args string) error {
	return s.publish(ctx, key, args, 0)
}

func (s *Svc) PublishDelay(ctx context.Context, key string, args string, delay int64) error {
	return s.publish(ctx, key, args, delay)
}

func (s *Svc) publish(ctx context.Context, key string, args string, delay int64) error {
	//ctx := context.Background()
	//ctx = context.WithValue(ctx, "opId", strings.ReplaceAll(uuid.New().String(), "-", ""))

	s.exp.Counter("publish_total").Inc()

	lg := CtxLogger(ctx).WithField("key", key)

	//s.mx.RLock()
	//_, ok := s.slot[name]
	//s.mx.RUnlock()

	//if !ok {
	//	err := fmt.Errorf("event: %s func is unregister", name)
	//	return err
	//}

	E := &event{
		Key:  key,
		Args: args,
		Id:   strings.ReplaceAll(uuid.New().String(), "-", ""),
	}

	EB, err := json.Marshal(E)
	if err != nil {
		lg.Errorf("marshal event failed: %v", err)
		return err
	}

	score := time.Now().Unix() + delay
	err = s.redis.ZAdd(ctx, s.key, redis.Z{Score: float64(score), Member: string(EB)}).Err()
	if err != nil {
		lg.WithError(err).Error("publish event failed")
		return err
	}

	return nil
}
