module logflow/ingestion-service

go 1.22

require (
	github.com/gin-gonic/gin v1.10.0
	github.com/google/uuid v1.6.0
	github.com/prometheus/client_golang v1.19.0
	github.com/segmentio/kafka-go v0.4.47
	github.com/segmentio/kafka-go/compress/zstd v0.0.0-20230921072822-ea5c54ba4038
	go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.50.0
	go.opentelemetry.io/otel v1.25.0
	go.uber.org/zap v1.27.0
)
