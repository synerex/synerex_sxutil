package sxutil

import (
	"flag"
	"log"

	//	"github.com/uber/jaeger-client-go"
	"go.opentelemetry.io/otel"

	//	stdout "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	jaegerHost = flag.String("jaegerHost", "127.0.0.1", "OpenTelemetry Jaeger host")
)

// Init configures an OpenTelemetry exporter and trace provider
func NewOltpTracer() *sdktrace.TracerProvider {
	//	exporter, err := stdout.New(stdout.WithPrettyPrint())
	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://" + *jaegerHost + ":14268/api/traces")))
	if err != nil {
		log.Fatal("Can't open jaeger tracer ", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp
}
