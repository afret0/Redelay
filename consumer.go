package Redelay

import (
	"context"
	"time"

	"github.com/afret0/wheel/log"
	"github.com/afret0/wheel/tool"
	"github.com/sirupsen/logrus"
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

}
