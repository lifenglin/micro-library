package helper

import (
	"sync"
	"time"
)

type Timer struct {
	duration sync.Map
}

func (t *Timer) Start(name string) {
	start := new(duration)
	start.Start()
	t.duration.Store(name, start)
}

func (t *Timer) End(name string) {
	if d, ok := t.duration.Load(name); ok {
		d.(*duration).End()
	}
}

func (t *Timer) Duration(name string) time.Duration {
	if d, ok := t.duration.Load(name); ok {
		return d.(*duration).Duration()
	} else {
		return 0
	}
}

func (t *Timer) Calculation() map[string]string {
	//转换时间格式
	var strTimer = make(map[string]string)
	var recordProfiler time.Duration
	allTimer := t.Duration("allTimer")

	t.duration.Range(func(key, value interface{}) bool {
		dura := value.(*duration).Duration()
		//精确到毫秒值
		strTimer[key.(string)] = dura.Round(time.Millisecond).String()
		if key != "allTimer" {
			//Timer标记过的总时间
			recordProfiler += dura
		}
		return true
	})

	//其余没有记录的百分比耗时
	strTimer["other"] = (allTimer - recordProfiler).Round(time.Millisecond).String()
	return strTimer
}

type duration struct {
	start    time.Time
	end      time.Time
	duration time.Duration
}

func (d *duration) Start() {
	d.start = time.Now()
}

func (d *duration) End() {
	d.end = time.Now()
}

func (d *duration) Duration() time.Duration {
	if d.end.IsZero() {
		d.End()
	}
	return d.end.Sub(d.start)
}
