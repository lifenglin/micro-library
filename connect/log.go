package connect

import (
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/lifenglin/micro-library/helper"
	"github.com/micro/go-micro/v2/config"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

var AccessLog *log.Logger
var SlowLog *log.Logger
var MysqlLog *log.Logger
var RedisLog *log.Logger
var stdErrFile *os.File

type logConfig struct {
	Level      string `json:"level"`
	Dirpath    string `json:"dirpath"`
	MysqlLevel string `json:"mysql_level"`
	Display    bool   `json:"display"`
}

func ConnectLog(srvName string) (err error) {
	var conf config.Config
	var watcher config.Watcher
	//启动时顺序问题，可能获取不到config，sleep+重试
	for i := 0; i < 3; i++ {
		conf, watcher, err = ConnectConfig(srvName, "log")
		if err != nil {
			if i == 2 {
				//配置获取失败
				log.Fatalf("读取consul配置错误: %s。如果在本地测试，可以使用sh import.sh导入配置到consul", err)
			}
			time.Sleep(time.Duration(5) * time.Second)
		}
	}
	var logConfig logConfig
	conf.Get(srvName, "log").Scan(&logConfig)

	//设置日志级别
	levelText := logConfig.Level
	level, err := log.ParseLevel(levelText)
	if err != nil {
		level, err = log.ParseLevel("info")
	}
	log.SetLevel(level)
	log.SetReportCaller(true)

	dir := filepath.Join(helper.GetBasePath(), logConfig.Dirpath, os.Getenv("POD_NAME"))
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatal(err)
		}
	}

	writerMap := make(lfshook.WriterMap)
	for _, logLevel := range log.AllLevels {
		path := filepath.Join(helper.GetBasePath(), logConfig.Dirpath, os.Getenv("POD_NAME"), logLevel.String()+".log")
		writer, err := rotatelogs.New(
			path+".%Y%m%d%H",
			rotatelogs.WithLinkName(path),
			rotatelogs.WithMaxAge(7*24*time.Hour),
			rotatelogs.WithRotationTime(time.Hour),
		)
		if err != nil {
			//日志write失败
			log.Fatal(err)
		}
		writerMap[logLevel] = writer
	}

	log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	log.StandardLogger().AddHook(lfshook.NewHook(
		writerMap,
		&log.TextFormatter{},
	))

	AccessLog = log.New()
	writerMap = make(lfshook.WriterMap)
	path := filepath.Join(helper.GetBasePath(), logConfig.Dirpath, os.Getenv("POD_NAME"), "access.log")
	writer, err := rotatelogs.New(
		path+".%Y%m%d%H",
		rotatelogs.WithLinkName(path),
		rotatelogs.WithMaxAge(24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour),
	)
	if err != nil {
		//日志write失败
		log.Fatal(err)
	}
	for _, logLevel := range log.AllLevels {
		writerMap[logLevel] = writer
	}
	AccessLog.AddHook(lfshook.NewHook(
		writerMap,
		&log.TextFormatter{},
	))

	SlowLog = log.New()
	writerMap = make(lfshook.WriterMap)
	path = filepath.Join(helper.GetBasePath(), logConfig.Dirpath, os.Getenv("POD_NAME"), "slow.log")
	writer, err = rotatelogs.New(
		path+".%Y%m%d%H",
		rotatelogs.WithLinkName(path),
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour),
	)
	if err != nil {
		//日志write失败
		log.Fatal(err)
	}
	for _, logLevel := range log.AllLevels {
		writerMap[logLevel] = writer
	}
	SlowLog.AddHook(lfshook.NewHook(
		writerMap,
		&log.TextFormatter{},
	))

	MysqlLog = log.New()
	MysqlLog.SetReportCaller(true)
	MysqlLog.SetLevel(level)
	writerMap = make(lfshook.WriterMap)
	path = filepath.Join(helper.GetBasePath(), logConfig.Dirpath, os.Getenv("POD_NAME"), "mysql.log")
	writer, err = rotatelogs.New(
		path+".%Y%m%d%H",
		rotatelogs.WithLinkName(path),
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour),
	)
	if err != nil {
		//日志write失败
		log.Fatal(err)
	}
	for _, logLevel := range log.AllLevels {
		writerMap[logLevel] = writer
	}
	MysqlLog.AddHook(lfshook.NewHook(
		writerMap,
		&log.TextFormatter{},
	))

	RedisLog = log.New()
	RedisLog.SetLevel(level)
	RedisLog.SetReportCaller(true)
	writerMap = make(lfshook.WriterMap)
	path = filepath.Join(helper.GetBasePath(), logConfig.Dirpath, os.Getenv("POD_NAME"), "redis.log")
	writer, err = rotatelogs.New(
		path+".%Y%m%d%H",
		rotatelogs.WithLinkName(path),
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour),
	)
	if err != nil {
		//日志write失败
		log.Fatal(err)
	}
	for _, logLevel := range log.AllLevels {
		writerMap[logLevel] = writer
	}
	RedisLog.AddHook(lfshook.NewHook(
		writerMap,
		&log.TextFormatter{},
	))

	go func() {
		v, err := watcher.Next()
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"file":  string(v.Bytes()),
			}).Warn("reconnect log")
		} else {
			log.WithFields(log.Fields{
				"file": string(v.Bytes()),
			}).Info("reconnect log")
		}
		ConnectLog(srvName)
		return
	}()

	if false == logConfig.Display {
		AccessLog.SetOutput(ioutil.Discard)
		log.SetOutput(ioutil.Discard)
		MysqlLog.SetOutput(ioutil.Discard)
		RedisLog.SetOutput(ioutil.Discard)
		SlowLog.SetOutput(ioutil.Discard)
	} else {
		AccessLog.SetOutput(os.Stderr)
		log.SetOutput(os.Stderr)
		MysqlLog.SetOutput(os.Stderr)
		RedisLog.SetOutput(os.Stderr)
		SlowLog.SetOutput(os.Stderr)
	}

	return nil
}

func ConnectStdLog(srvName string) (err error) {
	var conf config.Config
	var logConfig logConfig
	conf, _, err = ConnectConfig(srvName, "log")
	conf.Get(srvName, "log").Scan(&logConfig)

	//设置日志级别
	levelText := logConfig.Level
	level, err := log.ParseLevel(levelText)
	if err != nil {
		level, err = log.ParseLevel("info")
	}
	log.SetLevel(level)
	log.SetReportCaller(true)

	dir := filepath.Join(helper.GetBasePath(), logConfig.Dirpath, os.Getenv("POD_NAME"))
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatal(err)
		}
	}

	if stdErrFile == nil && false == logConfig.Display {
		stdErrFile, err = os.OpenFile(filepath.Join(helper.GetBasePath(), logConfig.Dirpath, os.Getenv("POD_NAME"), "stderr.log"), os.O_WRONLY|os.O_CREATE|os.O_SYNC|os.O_APPEND, 0666)
		if err != nil {
			//日志write失败
			log.Fatal(err)
		}
		err = syscall.Dup2(int(stdErrFile.Fd()), int(os.Stderr.Fd()))
		if err != nil {
			//日志write失败
			log.Fatal(err)
		}
		err = syscall.Dup2(int(stdErrFile.Fd()), int(os.Stdout.Fd()))
		if err != nil {
			//日志write失败
			log.Fatal(err)
		}
	}

	return nil
}
