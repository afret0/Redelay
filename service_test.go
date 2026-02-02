package delayTask

import (
	"fmt"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func prometheusHandler() gin.HandlerFunc {
	h := promhttp.Handler()
	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}

func RegisterPrometheusRouter(E *gin.Engine) {
	E.GET("/metrics", prometheusHandler())
}

func EF(args string) error {
	lg.Infof("args: %v", args)
	//panic("lsakdjlfkajlsd")
	//time.Sleep(10 * time.Minute)
	//return nil
	return nil
}

func TestService(t *testing.T) {
	//ctx := context.Background()

	RC := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{"r-bp1kvud328x48r9xp6pd.redis.rds.aliyuncs.com:6379"},
		Password: "Qiyiguo0621",
		Username: "kiwi0621",
	})

	go func() {
		e := gin.Default()
		RegisterPrometheusRouter(e)
		e.Run(":8080")
	}()

	//InitService("test", RC)

	svr1 := NewService("test", RC)
	for i := 0; i <= 100; i++ {
		event := fmt.Sprintf("test:event:%d", i)
		svr1.RegisterEventFunc(event, EF)

		time.Sleep(3 * time.Second)
		err := svr1.RegisterEvent(event, fmt.Sprintf("%d", i), int64(2))
		if err != nil {
			t.Error(err)
		}
	}

	time.Sleep(100 * time.Second)

}
