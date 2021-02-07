package library

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lifenglin/micro-library/connect"
	"github.com/lifenglin/micro-library/helper"
	"github.com/sirupsen/logrus"
	"path/filepath"
	"reflect"
	"strconv"
	"time"
)

func GetCache(ctx context.Context, hlp *helper.Helper, srvName string, name string, localCache bool, redisKey string, value interface{}) (err error) {
	log := hlp.RedisLog
	var bytes []byte
	if localCache {
		bigCache, err := connect.ConnectBigcache()
		if err == nil {
			bytes, err = bigCache.Get(filepath.Join(srvName, name, redisKey))
			if err == nil {
				err := json.Unmarshal(bytes, value)
				if err == nil {
					log.WithFields(logrus.Fields{
						"redisKey": redisKey,
						"value":    value,
						"bytes":    string(bytes),
					}).Trace("all hit local cache")
					return nil
				}
			}
		}
	}
	redis, err := connect.ConnectRedis(ctx, hlp, srvName, name)
	if err != nil {
		return err
	}
	bytes, err = redis.Get(redisKey).Bytes()
	if err != nil && err.Error() != "redis: nil" {
		log.WithFields(logrus.Fields{
			"error":    err,
			"redisKey": redisKey,
		}).Warn("getDataFromRedis error")
	} else if err != nil {
		//缓存未命中，从数据库中获取数据
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
			"bytes":    string(bytes),
		}).Trace("miss cache")
		return err
	}
	//如果命中缓存，则从缓存中拿出数据返回
	if bytes != nil {
		err := json.Unmarshal(bytes, value)
		if err != nil {
			log.WithFields(logrus.Fields{
				"error":    err,
				"redisKey": redisKey,
			}).Warn("json unmarshal error")
			return err
		}
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
			"value":    value,
			"bytes":    string(bytes),
		}).Trace("all hit cache")

		if localCache {
			bigCache, err := connect.ConnectBigcache()
			if err == nil {
				err = bigCache.Set(filepath.Join(srvName, name, redisKey), bytes)
				if err != nil {
					log.WithFields(logrus.Fields{
						"redisKey": redisKey,
						"bytes":    bytes,
						"error":    err,
					}).Warn("setLocal error")
				}
			}
		}
		return nil
	}
	return errors.New("redis: nil")
}

func MgetCache(ctx context.Context, hlp *helper.Helper, srvName string, name string, localCache bool, redisKey []string, value interface{}) (noCacheIndex []int, err error) {
	slice := reflect.ValueOf(value)
	if slice.Kind() != reflect.Slice {
		for key, _ := range redisKey {
			noCacheIndex = append(noCacheIndex, key)
		}
		return noCacheIndex, errors.New("value need slice")
	}
	noCacheIndex = make([]int, 0)
	if len(redisKey) != slice.Len() {
		return noCacheIndex, errors.New("len is not eq")
	}
	for key, item := range redisKey {
		err := GetCache(ctx, hlp, srvName, name, localCache, item, slice.Index(key).Interface())
		if err != nil {
			noCacheIndex = append(noCacheIndex, key)
		}
	}
	return noCacheIndex, nil
}

func MsetCache(ctx context.Context, hlp *helper.Helper, srvName string, name string, localCache bool, redisKey []string, value []interface{}, expire time.Duration) (err error) {
	if len(redisKey) != len(value) {
		return errors.New("len is not eq")
	}
	for key, item := range redisKey {
		SetCache(ctx, hlp, srvName, name, localCache, item, value[key], expire)
	}
	return nil
}

func GetCacheNum(ctx context.Context, hlp *helper.Helper, srvName string, name string, localCache bool, redisKey string) (num int64, err error) {
	log := hlp.RedisLog
	var bytes []byte
	if localCache {
		bigCache, err := connect.ConnectBigcache()
		if err == nil {
			bytes, err = bigCache.Get(filepath.Join(srvName, name, redisKey))
			if err == nil {
				int64, err := strconv.ParseInt(string(bytes), 10, 64)
				if err == nil {
					log.WithFields(logrus.Fields{
						"redisKey": redisKey,
						"value":    int64,
						"bytes":    string(bytes),
					}).Trace("all hit local cache")
					return int64, nil
				}
			}
		}
	}

	redis, err := connect.ConnectRedis(ctx, hlp, srvName, name)
	if err != nil {
		return num, err
	}
	num, err = redis.Get(redisKey).Int64()
	if err != nil && err.Error() != "redis: nil" {
		log.WithFields(logrus.Fields{
			"error":    err,
			"redisKey": redisKey,
		}).Warn("getDataFromRedis error")
		return num, errors.New("redis: nil")
	} else if err != nil {
		//缓存未命中，从数据库中获取数据
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
			"err":      err,
			"num":      num,
		}).Trace("miss cache")
		return num, errors.New("redis: nil")
	}
	if localCache {
		bigCache, err := connect.ConnectBigcache()
		if err == nil {
			err = bigCache.Set(filepath.Join(srvName, name, redisKey), []byte(fmt.Sprint(num)))
			if err != nil {
				log.WithFields(logrus.Fields{
					"redisKey": redisKey,
					"num":      num,
					"error":    err,
				}).Warn("setLocal error")
			}
		}
	}
	//如果命中缓存，则从缓存中拿出数据返回
	log.WithFields(logrus.Fields{
		"redisKey": redisKey,
		"num":      num,
	}).Trace("all hit cache")
	return num, nil
}

func DelCache(ctx context.Context, hlp *helper.Helper, srvName string, name string, redisKey string) (err error) {
	log := hlp.RedisLog
	redis, err := connect.ConnectRedis(ctx, hlp, srvName, name)
	if err != nil {
		return err
	}
	err = redis.Del(redisKey).Err()
	log.WithFields(logrus.Fields{
		"error":    err,
		"redisKey": redisKey,
	}).Trace("del redis")
	if nil != err {
		log.WithFields(logrus.Fields{
			"error":    err,
			"redisKey": redisKey,
		}).Warn("del error")
		return err
	}
	return nil
}

func SetCache(ctx context.Context, hlp *helper.Helper, srvName string, name string, localCache bool, redisKey string, value interface{}, expire time.Duration) (err error) {
	log := hlp.RedisLog
	redis, err := connect.ConnectRedis(ctx, hlp, srvName, name)
	if err != nil {
		return err
	}

	redisBytes, err := json.Marshal(value)
	if err != nil {
		log.WithFields(logrus.Fields{
			"error":    err,
			"redisKey": redisKey,
		}).Warn("json marshal error")
	}
	err = redis.Set(redisKey, redisBytes, expire).Err()
	if err != nil {
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
			"error":    err,
			"value":    value,
			"expire":   expire,
		}).Warn("setRedis error")
		return err
	} else {
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
			"value":    value,
			"string":   string(redisBytes),
			"expire":   expire,
		}).Trace("set redis")
	}

	if localCache {
		bigCache, err := connect.ConnectBigcache()
		if err == nil {
			err = bigCache.Set(filepath.Join(srvName, name, redisKey), redisBytes)
			if err != nil {
				log.WithFields(logrus.Fields{
					"redisKey": redisKey,
					"bytes":    redisBytes,
					"error":    err,
				}).Warn("setLocal error")
			}
		}
	}

	return nil
}

func SetCacheNum(ctx context.Context, hlp *helper.Helper, srvName string, name string, localCache bool, redisKey string, value int64, expire time.Duration) (err error) {
	log := hlp.RedisLog
	redis, err := connect.ConnectRedis(ctx, hlp, srvName, name)
	if err != nil {
		return err
	}

	err = redis.Set(redisKey, value, expire).Err()
	if err != nil {
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
			"error":    err,
			"value":    value,
			"expire":   expire,
		}).Warn("setRedis error")
		return err
	} else {
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
			"value":    value,
			"expire":   expire,
		}).Trace("set redis")
	}

	if localCache {
		bigCache, err := connect.ConnectBigcache()
		if err == nil {
			err = bigCache.Set(filepath.Join(srvName, name, redisKey), []byte(fmt.Sprint(value)))
			if err != nil {
				log.WithFields(logrus.Fields{
					"redisKey": redisKey,
					"bytes":    value,
					"error":    err,
				}).Warn("setLocal error")
			}
		}
	}

	return nil
}

// incr不主动更新本地缓存，如果更新本地缓存，可能出现数字时而大时而小的情况
func IncrCacheNum(ctx context.Context, hlp *helper.Helper, srvName string, name string, redisKey string) (err error) {
	log := hlp.RedisLog
	redis, err := connect.ConnectRedis(ctx, hlp, srvName, name)
	if err != nil {
		return err
	}

	err = redis.Incr(redisKey).Err()
	if err != nil {
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
			"error":    err,
		}).Warn("incrRedis error")
		return err
	} else {
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
		}).Trace("incr redis")
	}

	return nil
}

// decr不主动更新本地缓存，如果更新本地缓存，可能出现数字时而大时而小的情况
func DecrCacheNum(ctx context.Context, hlp *helper.Helper, srvName string, name string, redisKey string) (err error) {
	log := hlp.RedisLog
	redis, err := connect.ConnectRedis(ctx, hlp, srvName, name)
	if err != nil {
		return err
	}

	err = redis.Decr(redisKey).Err()
	if err != nil {
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
			"error":    err,
		}).Warn("incrRedis error")
		return err
	} else {
		log.WithFields(logrus.Fields{
			"redisKey": redisKey,
		}).Trace("incr redis")
	}

	return nil
}
