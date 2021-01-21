package connect

import (
	"github.com/micro/go-micro/v2/config"
	"github.com/segmentio/kafka-go"
	"strings"
	"sync"
	"time"
)

var (
	brokerMap sync.Map
	locker    sync.Mutex
)

//async设置为true，表示不阻塞， 不需要等待返回值确认
func GetKafkaWriter(svrName, name, topic string, async bool) (*kafka.Writer, error) {
	brokers, err := getBrokers(svrName, name, topic)
	if err != nil {
		return nil, err
	}

	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    async,
	}), nil
}

func GetKafkaReader(svrName, name, topic, groupID string) (*kafka.Reader, error) {
	brokers, err := getBrokers(svrName, name, topic)
	if err != nil {
		return nil, err
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	}), nil
}

func getBrokers(svrName, name, topic string) ([]string, error) {
	key := name + "." + topic
	brokerValue, ok := brokerMap.Load(key)
	if !ok {
		locker.Lock()
		defer locker.Unlock()
		brokerValue, ok := brokerMap.Load(key)
		if ok {
			return brokerValue.([]string), nil
		}

		conf, watcher, err := ConnectConfig(svrName, "kafka")
		if err != nil {
			return nil, err
		}

		var brokerAddress string
		err = conf.Get(svrName, "kafka", name, topic).Scan(&brokerAddress)
		if err != nil {
			return nil, err
		}

		go deleteBrokerOnUpdate(watcher, key)

		brokers := strings.Split(brokerAddress, ",")
		brokerMap.Store(key, brokers)
		return brokers, nil
	}

	return brokerValue.([]string), nil
}

func deleteBrokerOnUpdate(watcher config.Watcher, key string) {
	for {
		_, err := watcher.Next()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		brokerMap.Delete(key)
		break
	}
}
