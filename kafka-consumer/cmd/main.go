// cmd/main.go — Kafka Consumer Service entrypoint.
package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"logflow/kafka-consumer/internal/consumer"
	"logflow/kafka-consumer/internal/writer"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync() //nolint:errcheck

	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	chHosts := strings.Split(getEnv("CLICKHOUSE_HOSTS", "localhost:9000"), ",")

	writerCfg := writer.DefaultConfig(chHosts)
	writerCfg.Username = getEnv("CLICKHOUSE_USER", "logflow_writer")
	writerCfg.Password = getEnv("CLICKHOUSE_PASSWORD", "")
	writerCfg.Database = getEnv("CLICKHOUSE_DB", "logflow")

	chWriter, err := writer.New(writerCfg, log)
	if err != nil {
		log.Fatal("clickhouse writer init failed", zap.Error(err))
	}
	defer chWriter.Close()

	consumerCfg := consumer.Config{
		Brokers:      kafkaBrokers,
		Topic:        getEnv("KAFKA_TOPIC", "logs-raw"),
		GroupID:      getEnv("KAFKA_GROUP_ID", "logflow-consumer-group"),
		DLQTopic:     getEnv("KAFKA_DLQ_TOPIC", "logs-dlq"),
		Workers:      getEnvInt("CONSUMER_WORKERS", 4),
		BatchSize:    getEnvInt("CONSUMER_BATCH_SIZE", 2000),
		BatchTimeout: time.Duration(getEnvInt("CONSUMER_BATCH_TIMEOUT_MS", 500)) * time.Millisecond,
		MaxRetries:   getEnvInt("MAX_RETRIES", 5),
	}

	c := consumer.New(consumerCfg, chWriter, log)

	ctx, cancel := context.WithCancel(context.Background())
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Info("shutting down kafka-consumer")
		cancel()
	}()

	log.Info("kafka-consumer starting",
		zap.Strings("brokers", kafkaBrokers),
		zap.String("topic", consumerCfg.Topic),
		zap.Int("workers", consumerCfg.Workers),
	)
	c.Run(ctx)
	log.Info("kafka-consumer stopped")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
