package connect

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/lifenglin/micro-library/helper"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

var rds *Rds

type Rds struct {
	sync.RWMutex
	Map map[string]*redis.ClusterClient
}

func init() {
	rds = new(Rds)
	rds.Map = make(map[string]*redis.ClusterClient)
}

func ConnectRedis(ctx context.Context, hlp *helper.Helper, srvName string, name string) (*redis.ClusterClient, error) {
	timer := hlp.Timer
	timer.Start("connectRedis")
	defer timer.End("connectRedis")

	rds.RLock()
	rd, ok := rds.Map[name]
	rds.RUnlock()
	if !ok {
		rds.Lock()
		existRd, ok := rds.Map[name]
		if ok {
			rd = existRd
		} else {
			conf, watcher, err := ConnectConfig(srvName, "redis")
			if err != nil {
				hlp.RedisLog.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Error("read redis config fail")
				return nil, fmt.Errorf("read redis config fail: %w", err)
			}

			var clusterConfig redis.ClusterOptions
			conf.Get(srvName, "redis", name).Scan(&clusterConfig)

			rd = redis.NewClusterClient(&clusterConfig)

			pong, err := rd.Ping().Result()
			if err != nil {
				hlp.RedisLog.WithFields(logrus.Fields{
					"addr":  clusterConfig.Addrs,
					"pong":  pong,
					"error": err.Error(),
				}).Error("connect redis fail")
				return nil, fmt.Errorf("connect redis fail: %w", err)
			}
			rds.Map[name] = rd

			go func() {
				v, err := watcher.Next()
				if err != nil {
					hlp.RedisLog.WithFields(logrus.Fields{
						"error": err,
						"name":  name,
						"file":  string(v.Bytes()),
					}).Warn("reconect redis")
				} else {
					hlp.RedisLog.WithFields(logrus.Fields{
						"name": name,
						"file": string(v.Bytes()),
					}).Info("reconnect redis")

					//配置更新了，释放所有已有的rd对象，关闭连接
					rds.RLock()
					rd, ok := rds.Map[name]
					rds.RUnlock()
					if !ok {
						return
					}

					rds.Lock()
					delete(rds.Map, name)
					rds.Unlock()
					//10秒后，关闭旧的redis连接
					time.Sleep(time.Duration(10) * time.Second)
					err = rd.Close()
					if err == nil {
						hlp.RedisLog.WithFields(logrus.Fields{
							"name": name,
							"file": string(v.Bytes()),
						}).Info("close rds")
					} else {
						hlp.RedisLog.WithFields(logrus.Fields{
							"error": err,
							"name":  name,
							"file":  string(v.Bytes()),
						}).Warn("close rds error")
					}
				}
				return
			}()
		}
		rds.Unlock()
	}
	newRedis := rd.WithContext(ctx)
	return newRedis, nil
}
