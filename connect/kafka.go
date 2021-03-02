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
	readerMap sync.Map
	writerMap sync.Map
	locker    sync.Mutex
)

//async设置为true，表示不阻塞， 不需要等待返回值确认
func GetKafkaWriter(svrName, name, topic string, async bool) (*kafka.Writer, error) {
	key := keyName(name, topic)
	if writer, ok := writerMap.Load(key); ok {
		return writer.(*kafka.Writer), nil
	}

	brokers, err := getBrokers(svrName, name, topic)
	if err != nil {
		return nil, err
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    async,
	})

	writerMap.Store(key, writer)

	return writer, nil
}

func GetKafkaReader(svrName, name, topic, groupID string) (*kafka.Reader, error) {
	key := keyName(name, topic)
	if reader, ok := readerMap.Load(key); ok {
		return reader.(*kafka.Reader), nil
	}

	brokers, err := getBrokers(svrName, name, topic)
	if err != nil {
		return nil, err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	readerMap.Store(key, reader)

	return reader, nil
}

func getBrokers(svrName, name, topic string) ([]string, error) {
	key := keyName(name, topic)
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

func keyName(name, topic string) string {
	return name + "." + topic
}

func deleteBrokerOnUpdate(watcher config.Watcher, key string) {
	for {
		_, err := watcher.Next()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		brokerMap.Delete(key)
		writerMap.Delete(key)
		readerMap.Delete(key)
		break
	}
}
