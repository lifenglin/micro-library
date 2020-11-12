package library

import (
	"context"
	"github.com/go-redis/redis_rate"
	"github.com/lifenglin/micro-library/connect"
	"github.com/lifenglin/micro-library/helper"
	"time"
)

func RedisRate(ctx context.Context, hlp *helper.Helper, srvName string, name string, redisKey string, dur time.Duration, maxn int64) (count int64, delay time.Duration, allow bool, err error) {
	redis, err := connect.ConnectRedis(ctx, hlp, srvName, name)
	if err != nil {
		return 0, 0, false, err
	}

	limiter := redis_rate.NewLimiter(redis)
	count, delay, allow = limiter.Allow(redisKey, maxn, dur)

	return count, delay, allow, nil
}