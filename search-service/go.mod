module logflow/search-service

go 1.22

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.23.0
	github.com/gin-gonic/gin v1.10.0
	github.com/prometheus/client_golang v1.19.0
	github.com/redis/go-redis/v9 v9.5.1
	go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.50.0
	go.opentelemetry.io/otel v1.25.0
	go.uber.org/zap v1.27.0
)
