package connect

import (
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"log"
	"time"
)

func InitJaeger(srvName string) {
	sender, err := jaeger.NewUDPTransport("jaeger-agent.default:5775", 0)
	if err != nil {
		log.Println("connect jaeger err: ", err)
		return
	}

	tracer, _ := jaeger.NewTracer(
		srvName,
		jaeger.NewRateLimitingSampler(1),
		jaeger.NewRemoteReporter(
			sender,
			jaeger.ReporterOptions.BufferFlushInterval(60*time.Second),
		),
	)

	opentracing.SetGlobalTracer(tracer)
}
