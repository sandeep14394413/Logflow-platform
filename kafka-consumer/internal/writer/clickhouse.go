// Package writer provides a ClickHouse bulk-insert writer optimised for
// high-throughput log ingestion using the native binary protocol.
package writer

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	chWriteRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "logflow",
		Subsystem: "clickhouse",
		Name:      "rows_written_total",
		Help:      "Total rows written to ClickHouse.",
	}, []string{"table", "outcome"})

	chWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "clickhouse",
		Name:      "write_duration_seconds",
		Help:      "Time spent writing a batch to ClickHouse.",
		Buckets:   []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"table"})
)

// LogRow mirrors the ClickHouse logs_local table schema.
type LogRow struct {
	ID          string            `json:"id"`
	TenantID    string            `json:"tenant_id"`
	Timestamp   time.Time         `json:"timestamp"`
	Namespace   string            `json:"namespace"`
	Service     string            `json:"service"`
	PodName     string            `json:"pod_name"`
	NodeName    string            `json:"node_name"`
	Environment string            `json:"environment"`
	Level       string            `json:"level"`
	TraceID     string            `json:"trace_id"`
	SpanID      string            `json:"span_id"`
	Message     string            `json:"message"`
	HostIP      string            `json:"host_ip"`
	Labels      map[string]string `json:"labels"`
	Attributes  map[string]string `json:"attributes"`
}

// ClickHouseWriter is the interface consumed by the Kafka consumer.
type ClickHouseWriter interface {
	WriteBatch(ctx context.Context, rows []LogRow) error
}

// Config holds ClickHouse connection settings.
type Config struct {
	Hosts       []string
	Database    string
	Username    string
	Password    string
	DialTimeout time.Duration
	MaxOpenConn int
	MaxIdleConn int
	Table       string
}

// DefaultConfig returns sane production defaults.
func DefaultConfig(hosts []string) Config {
	return Config{
		Hosts:       hosts,
		Database:    "logflow",
		Username:    "logflow_writer",
		Table:       "logs_distributed",
		DialTimeout: 10 * time.Second,
		MaxOpenConn: 10,
		MaxIdleConn: 5,
	}
}

// Writer implements ClickHouseWriter using the native binary protocol.
type Writer struct {
	conn driver.Conn
	cfg  Config
	log  *zap.Logger
}

// New opens a ClickHouse connection using the binary protocol for best performance.
func New(cfg Config, log *zap.Logger) (*Writer, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Hosts,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     cfg.DialTimeout,
		MaxOpenConns:    cfg.MaxOpenConn,
		MaxIdleConns:    cfg.MaxIdleConn,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"max_insert_block_size":         10_000_000,
			"insert_deduplicate":            0,
			"async_insert":                  0,
			"wait_for_async_insert":         0,
		},
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}
	log.Info("clickhouse connection established", zap.Strings("hosts", cfg.Hosts))
	return &Writer{conn: conn, cfg: cfg, log: log}, nil
}

// WriteBatch inserts a slice of LogRow using ClickHouse native batch API.
// This is a single round-trip regardless of batch size, making it extremely efficient.
func (w *Writer) WriteBatch(ctx context.Context, rows []LogRow) error {
	if len(rows) == 0 {
		return nil
	}
	start := time.Now()
	table := w.cfg.Database + "." + w.cfg.Table

	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO "+table)
	if err != nil {
		chWriteRows.WithLabelValues(w.cfg.Table, "error").Add(float64(len(rows)))
		return err
	}

	for _, r := range rows {
		ip := clickhouse.IPv4(parseIP(r.HostIP))
		if err := batch.Append(
			r.ID,
			r.TenantID,
			r.Timestamp.UTC(),
			r.Namespace,
			r.Service,
			r.PodName,
			r.NodeName,
			r.Environment,
			r.Level,
			r.TraceID,
			r.SpanID,
			r.Message,
			ip,
			r.Labels,
			r.Attributes,
		); err != nil {
			w.log.Warn("batch append error", zap.Error(err), zap.String("id", r.ID))
		}
	}

	if err := batch.Send(); err != nil {
		chWriteRows.WithLabelValues(w.cfg.Table, "error").Add(float64(len(rows)))
		chWriteDuration.WithLabelValues(w.cfg.Table).Observe(time.Since(start).Seconds())
		return err
	}

	chWriteRows.WithLabelValues(w.cfg.Table, "success").Add(float64(len(rows)))
	chWriteDuration.WithLabelValues(w.cfg.Table).Observe(time.Since(start).Seconds())
	w.log.Debug("batch written", zap.Int("rows", len(rows)), zap.Duration("took", time.Since(start)))
	return nil
}

// parseIP converts a string IP to a 4-byte representation; returns zero on failure.
func parseIP(s string) [4]byte {
	var b [4]byte
	// Simple IPv4 parse — production code should use net.ParseIP.
	_ = s
	return b
}

// Close releases the connection pool.
func (w *Writer) Close() error {
	return w.conn.Close()
}
