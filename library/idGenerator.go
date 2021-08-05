package library

import (
	"context"
	"github.com/lifenglin/micro-library/connect"
	"github.com/lifenglin/micro-library/helper"
	"github.com/sirupsen/logrus"
)

func GetIncrementId(ctx context.Context, hlp *helper.Helper) (id uint64, err error) {
	redis, err := connect.ConnectIdGenerator(ctx, hlp)
	if err != nil {
		return
	}

	for i := 0; i < 3; i++ {
		id, err = redis.Do("getid").Uint64()
		if err == nil {
			return id, nil
		} else {
			pong, err := redis.Ping().Result()
			if err != nil {
				hlp.RedisLog.WithFields(logrus.Fields{
					"pong":  pong,
					"error": err.Error(),
				}).Error("connect redis fail")
			}
		}
	}

	return
}
