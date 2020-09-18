package connect

import (
	"context"
	"github.com/lifenglin/micro-library/helper"
	"github.com/micro/go-micro/v2/config"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type MongoConfig struct {
	Addr            string `json:"addr"`
	MinPoolSize     uint64 `json:"min_pool_size"`
	MaxPoolSize     uint64 `json:"max_pool_size"`
	MaxConnIdleTime string `json:"max_conn_idle_time"`
	ConnectTimeout  string `json:"connect_timeout"`
}

var mongoDB sync.Map

func MongoDB(ctx context.Context, hlp *helper.Helper, srvName string, name string) (*mongo.Database, error) {
	logger := hlp.Log
	c, ok := mongoDB.Load(name)
	if ok {
		return c.(*mongo.Client).Database("local"), nil
	}

	client, err := newClient(ctx, srvName, name, logger)
	if err != nil {
		return nil, err
	}

	mongoDB.Store(name, client)
	return client.Database("local"), nil
}

func getOption(srvName, name string, logger *logrus.Entry) (config MongoConfig, watcher config.Watcher, err error) {
	conf, watcher, err := ConnectConfig(srvName, "mongo")
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error:": err,
		}).Error("get mongo option err")
		return
	}

	err = conf.Get(srvName, "mongo", name).Scan(&config)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error:": err,
		}).Error("scan mongo config error:", err)
		return
	}

	return
}

func newClient(ctx context.Context, srvName, name string, logger *logrus.Entry) (*mongo.Client, error) {
	conf, watcher, err := getOption(srvName, name, logger)
	if err != nil {
		return nil, err
	}

	maxConnIdleTime, err := time.ParseDuration(conf.MaxConnIdleTime)
	if err != nil {
		maxConnIdleTime = 600 * time.Second
	}
	connectTimeout, err := time.ParseDuration(conf.ConnectTimeout)
	if err != nil {
		connectTimeout = 1 * time.Second
	}

	o := &options.ClientOptions{
		AppName:         &srvName,
		MinPoolSize:     &conf.MinPoolSize,
		MaxPoolSize:     &conf.MaxPoolSize,
		MaxConnIdleTime: &maxConnIdleTime,
		ConnectTimeout:  &connectTimeout,
	}
	client, err := mongo.NewClient(options.Client().ApplyURI(conf.Addr), o)
	if err != nil {
		return nil, err
	}

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	go watch(ctx, watcher, name, logger)
	return client, nil
}

func watch(ctx context.Context, watcher config.Watcher, name string, logger *logrus.Entry) {
	v, err := watcher.Next()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"name:":  name,
			"error:": err,
		}).Error("mongo watch error:", err)
		return
	}

	logger.WithFields(logrus.Fields{
		"name": name,
		"file": string(v.Bytes()),
	}).Info("reconnect mongo db")

	c, ok := mongoDB.LoadAndDelete(name)
	if !ok {
		logger.WithFields(logrus.Fields{
			"name":   name,
			"error:": "not found",
		}).Error("mongo load and delete")
	}

	time.Sleep(time.Duration(10) * time.Second)
	err = c.(*mongo.Client).Disconnect(ctx)
	if err == nil {
		logger.WithFields(logrus.Fields{
			"name": name,
			"file": string(v.Bytes()),
		}).Info("close db")
	} else {
		logger.WithFields(logrus.Fields{
			"error": err,
			"name":  name,
			"file":  string(v.Bytes()),
		}).Warn("close db error")
	}
}
