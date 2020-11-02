package connect

import (
	"fmt"
	"github.com/micro/go-micro/v2/config"
	"github.com/micro/go-micro/v2/config/encoder/yaml"
	"github.com/micro/go-micro/v2/config/source"
	"github.com/micro/go-plugins/config/source/consul/v2"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"sync"
)

var configs *configMap

type configMap struct {
	sync.RWMutex
	Map     map[string]config.Config
	Watcher map[string]config.Watcher
}

func init() {
	configs = new(configMap)
	configs.Map = make(map[string]config.Config)
	configs.Watcher = make(map[string]config.Watcher)
}

func ConnectConfig(srvName string, confName string) (config.Config, config.Watcher, error) {
	configs.RLock()
	name := filepath.Join(srvName, confName)
	_, ok := configs.Map[name]
	configs.RUnlock()

	if !ok {
		configs.Lock()
		_, ok := configs.Map[name]
		if !ok {
			conf, watcher, err := newConfig(name)
			if err == nil {
				configs.Map[name] = conf
				configs.Watcher[name] = watcher
			}
			configs.Unlock()
			return conf, watcher, err
		}
		configs.Unlock()
	}

	return configs.Map[name], configs.Watcher[name], nil
}

func newConfig(name string) (config.Config, config.Watcher, error) {
	consulSource := consul.NewSource(
		consul.WithAddress(os.Getenv("CONSUL_ADDR")),
		consul.WithPrefix(name),
		consul.StripPrefix(false),
		source.WithEncoder(yaml.NewEncoder()),
	)
	conf, err := config.NewConfig()
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err.Error(),
			"name":  name,
		}).Error("read config fail")
		return conf, nil, fmt.Errorf("read config fail: %w", err)
	}
	err = conf.Load(consulSource)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err.Error(),
			"name":  name,
		}).Error("read config fail")
		return conf, nil, fmt.Errorf("read config fail: %w", err)
	}
	//配置发生变化了 执行响应的操作
	watcher, err := conf.Watch()
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Warn("watcher log error")
	}
	return conf, watcher, nil
}
