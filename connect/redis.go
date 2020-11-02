package connect

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/lifenglin/micro-library/helper"
	"github.com/sirupsen/logrus"
	"path/filepath"
	"sync"
	"time"
)

var rds *Rds

type Rds struct {
	sync.RWMutex
	Map      map[string]*redis.ClusterClient
	MapRedis map[string]*redis.Client
}

type RedisConf struct {
	Addrs        []string
	MaxRetries   int
	PoolSize     int
	MinIdleConns int
	DialTimeout  string
	ReadTimeout  string
	WriteTimeout string
	MaxConnAge   string
}

func (rc RedisConf) clusterOptions() (redis.ClusterOptions, error) {
	dial, err := time.ParseDuration(rc.DialTimeout)
	if err != nil {
		return redis.ClusterOptions{}, fmt.Errorf("dial timeout: %s", err)
	}
	read, err := time.ParseDuration(rc.ReadTimeout)
	if err != nil {
		return redis.ClusterOptions{}, fmt.Errorf("read timeout: %s", err)
	}
	write, err := time.ParseDuration(rc.WriteTimeout)
	if err != nil {
		return redis.ClusterOptions{}, fmt.Errorf("write timeout: %s", err)
	}
	maxConn, err := time.ParseDuration(rc.MaxConnAge)
	if err != nil {
		return redis.ClusterOptions{}, fmt.Errorf("max conn age: %s", err)
	}

	return redis.ClusterOptions{
		Addrs:        rc.Addrs,
		MaxRetries:   rc.MaxRetries,
		PoolSize:     rc.PoolSize,
		MinIdleConns: rc.MinIdleConns,
		DialTimeout:  dial,
		ReadTimeout:  read,
		WriteTimeout: write,
		MaxConnAge:   maxConn,
	}, nil
}

func init() {
	rds = new(Rds)
	rds.Map = make(map[string]*redis.ClusterClient)
	rds.MapRedis = make(map[string]*redis.Client)
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
			conf, watcher, err := newConfig(filepath.Join(srvName, "redis"))
			if err != nil {
				hlp.RedisLog.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Error("read redis config fail")
				rds.Unlock()
				return nil, fmt.Errorf("read redis config fail: %w", err)
			}

			var redisConfig RedisConf
			err = conf.Get(srvName, "redis", name).Scan(&redisConfig)
			if err != nil {
				hlp.RedisLog.WithFields(logrus.Fields{
					"srv name":   srvName,
					"redis name": name,
					"error":      err.Error(),
				}).Error("redis scan: ", err)
				rds.Unlock()
				return nil, fmt.Errorf("cluster config scan error: %w", err)
			}

			clusterConfig, err := redisConfig.clusterOptions()
			if err != nil {
				hlp.RedisLog.WithFields(logrus.Fields{
					"srv name":              srvName,
					"redis name":            name,
					"cluster options error": err.Error(),
				}).Error("get cluster options: ", err)
				rds.Unlock()
				return nil, fmt.Errorf("cluster config options error: %w", err)
			}

			rd = redis.NewClusterClient(&clusterConfig)

			pong, err := rd.Ping().Result()
			if err != nil {
				hlp.RedisLog.WithFields(logrus.Fields{
					"addr":  clusterConfig.Addrs,
					"pong":  pong,
					"error": err.Error(),
				}).Error("connect redis fail")
				rds.Unlock()
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

func ConnectIdGenerator(ctx context.Context, hlp *helper.Helper) (*redis.Client, error) {
	timer := hlp.Timer
	timer.Start("ConnectIdGenerator")
	defer timer.End("ConnectIdGenerator")
	srvName := "IdGenerator"
	name := "IdGenerator"

	rds.RLock()
	rd, ok := rds.MapRedis[name]
	rds.RUnlock()
	if !ok {
		rds.Lock()
		existRd, ok := rds.MapRedis[name]
		if ok {
			rd = existRd
		} else {
			conf, watcher, err := newConfig(filepath.Join(srvName, "redis"))
			if err != nil {
				hlp.RedisLog.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Error("read redis config fail")
				rds.Unlock()
				return nil, fmt.Errorf("read redis config fail: %w", err)
			}

			var config = new(redis.Options)
			conf.Get(srvName, "redis", name).Scan(config)
			rd = redis.NewClient(config)

			pong, err := rd.Ping().Result()
			if err != nil {
				hlp.RedisLog.WithFields(logrus.Fields{
					"addr":  config.Addr,
					"pong":  pong,
					"error": err.Error(),
				}).Error("connect redis fail")
				rds.Unlock()
				return nil, fmt.Errorf("connect redis fail: %w", err)
			}
			rds.MapRedis[name] = rd

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
					rd, ok := rds.MapRedis[name]
					rds.RUnlock()
					if !ok {
						return
					}

					rds.Lock()
					delete(rds.MapRedis, name)
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
