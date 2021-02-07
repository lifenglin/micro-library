package connect

import (
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"log"
	"os"
	"time"
)

func InitJaeger(srvName string) {
	jaegerAddress := os.Getenv("JAEGER_ADDRESS")
	if jaegerAddress == "" {
		return
	}

	sender, err := jaeger.NewUDPTransport(jaegerAddress, 0)
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
