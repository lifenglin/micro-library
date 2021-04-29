package connect

import (
	"context"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/lifenglin/micro-library/helper"
	"github.com/sirupsen/logrus"
	mysql2 "gorm.io/driver/mysql"
	gorm2 "gorm.io/gorm"
	"gorm.io/plugin/prometheus"
	"path/filepath"
	"sync"
	"time"
)

var dbs *Dbs

type Dbs struct {
	sync.RWMutex
	Map map[string]*gorm.DB
}

func init() {
	dbs = new(Dbs)
	dbs.Map = make(map[string]*gorm.DB)
}

type mysqlClusterConfig struct {
	ConnMaxLifetime int    `json:"conn_max_lifetime"`
	Dsn             string `json:"dsn"`
	MaxIdleConns    int    `json:"max_idle_conns"`
	MaxOpenConns    int    `json:"max_open_conns"`
}

func MysqlInit(srvName string) {
	hlp := new(helper.Helper)
	hlp.Timer = new(helper.Timer)
	hlp.MysqlLog = MysqlLog.WithTime(time.Now())

	conf, _, err := newConfig(filepath.Join(srvName, "database"))
	if err != nil {
		hlp.MysqlLog.WithFields(logrus.Fields{
			"err": err,
		}).Error("get config")
		return
	}

	var config map[string]interface{}
	err = conf.Get(srvName, "database").Scan(&config)
	if err != nil {
		hlp.MysqlLog.WithFields(logrus.Fields{
			"err": err,
		}).Error("scan config")
		return
	}

	for k, _ := range config {
		db, err := ConnectDB(context.Background(), hlp, srvName, k, "master")
		if err != nil {
			hlp.MysqlLog.WithFields(logrus.Fields{
				"name":    k,
				"err":     err,
				"cluster": "master",
			}).Error("connect mysql error")
		} else {
			for i := 0; i < 10; i++ {
				_, _ = db.DB().Conn(context.Background())
			}
		}

		db, err = ConnectDB(context.Background(), hlp, srvName, k, "slave")
		if err != nil {
			hlp.MysqlLog.WithFields(logrus.Fields{
				"name":    k,
				"err":     err,
				"cluster": "slave",
			}).Error("connect mysql error")
		} else {
			for i := 0; i < 10; i++ {
				_, _ = db.DB().Conn(context.Background())
			}
		}
	}
}

func ConnectDB(ctx context.Context, hlp *helper.Helper, srvName string, name string, cluster string) (*gorm.DB, error) {
	timer := hlp.Timer
	timer.Start("connectDB")
	defer timer.End("connectDB")

	dbsKey := name + "." + cluster
	mysqlLog := hlp.MysqlLog
	dbs.RLock()
	db, ok := dbs.Map[dbsKey]
	dbs.RUnlock()
	if !ok {
		dbs.Lock()
		existDb, ok := dbs.Map[dbsKey]
		if ok {
			db = existDb
		} else {
			conf, watcher, err := newConfig(filepath.Join(srvName, "database"))
			if err != nil {
				mysqlLog.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Error("read database config fail")
				dbs.Unlock()
				return nil, fmt.Errorf("read database config fail: %w", err)
			}
			var clusterConfig mysqlClusterConfig
			conf.Get(srvName, "database", name, cluster).Scan(&clusterConfig)
			mysqlLog.WithFields(logrus.Fields{
				"srvName": srvName,
				"name":    name,
				"cluster": cluster,
				"dsn":     clusterConfig.Dsn,
			}).Info("connect mysql info")

			db, err = gorm.Open("mysql", clusterConfig.Dsn)
			if err != nil {
				mysqlLog.WithFields(logrus.Fields{
					"dsn":   clusterConfig.Dsn,
					"error": err.Error(),
				}).Error("connect mysql fail")
				dbs.Unlock()
				return nil, fmt.Errorf("connect mysql fail: %w", err)
			}
			//设置连接池
			db.DB().SetMaxIdleConns(clusterConfig.MaxIdleConns)
			db.DB().SetMaxOpenConns(clusterConfig.MaxOpenConns)
			db.DB().SetConnMaxLifetime(time.Duration(clusterConfig.ConnMaxLifetime) * time.Second)
			db.SingularTable(true)
			db.BlockGlobalUpdate(false)
			dbs.Map[dbsKey] = db

			db2, err := gorm2.Open(mysql2.Open(clusterConfig.Dsn), &gorm2.Config{})
			if err == nil {
				db2.ConnPool = db.DB()
				db2.Use(prometheus.New(prometheus.Config{
					DBName: srvName,
					MetricsCollector: []prometheus.MetricsCollector{
						&prometheus.MySQL{VariableNames: []string{"Threads_running"}},
					},
				}))
			}

			go func() {
				v, err := watcher.Next()
				if err != nil {
					mysqlLog.WithFields(logrus.Fields{
						"error":   err,
						"name":    name,
						"cluster": cluster,
						"file":    string(v.Bytes()),
					}).Warn("reconect db")
				} else {
					mysqlLog.WithFields(logrus.Fields{
						"name":    name,
						"cluster": cluster,
						"file":    string(v.Bytes()),
					}).Info("reconnect db")

					//配置更新了，释放所有已有的dbs对象，关闭连接
					dbs.RLock()
					db, ok := dbs.Map[dbsKey]
					dbs.RUnlock()
					if !ok {
						return
					}

					dbs.Lock()
					delete(dbs.Map, dbsKey)
					dbs.Unlock()
					//10秒后，关闭旧的数据库连接
					time.Sleep(time.Duration(10) * time.Second)
					err = db.Close()
					if err == nil {
						mysqlLog.WithFields(logrus.Fields{
							"name":    name,
							"cluster": cluster,
							"file":    string(v.Bytes()),
						}).Info("close db")
					} else {
						mysqlLog.WithFields(logrus.Fields{
							"error":   err,
							"name":    name,
							"cluster": cluster,
							"file":    string(v.Bytes()),
						}).Warn("close db error")
					}
				}
				return
			}()
		}
		dbs.Unlock()
	}
	newDb := db.New()
	newDb.SetLogger(mysqlLog)
	conf, _, err := ConnectConfig(srvName, "log")
	if err != nil {
		//配置获取失败
		mysqlLog.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Error("read log config fail")
	} else {
		newDb.LogMode(conf.Get(srvName, "log", "mysql_detailed_log").Bool(false))
	}
	return newDb, nil
}
