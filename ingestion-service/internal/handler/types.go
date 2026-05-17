// Package types provides shared data structures across all LogFlow services.
package types

import "time"

// LogEntry is the canonical log record written to ClickHouse and streamed via Kafka.
type LogEntry struct {
	ID          string            `json:"id"           ch:"id"`
	TenantID    string            `json:"tenant_id"    ch:"tenant_id"`
	Timestamp   time.Time         `json:"timestamp"    ch:"timestamp"`
	Level       string            `json:"level"        ch:"level"`
	Service     string            `json:"service"      ch:"service"`
	Namespace   string            `json:"namespace"    ch:"namespace"`
	PodName     string            `json:"pod_name"     ch:"pod_name"`
	NodeName    string            `json:"node_name"    ch:"node_name"`
	TraceID     string            `json:"trace_id"     ch:"trace_id"`
	SpanID      string            `json:"span_id"      ch:"span_id"`
	Message     string            `json:"message"      ch:"message"`
	Labels      map[string]string `json:"labels"       ch:"labels"`
	Attributes  map[string]string `json:"attributes"   ch:"attributes"`
	HostIP      string            `json:"host_ip"      ch:"host_ip"`
	Environment string            `json:"environment"  ch:"environment"`
}

// IngestRequest is the HTTP payload for bulk log ingestion.
type IngestRequest struct {
	Logs []LogEntry `json:"logs" validate:"required,min=1,max=10000"`
}

// IngestResponse is returned after successful ingestion.
type IngestResponse struct {
	Accepted  int    `json:"accepted"`
	Dropped   int    `json:"dropped"`
	RequestID string `json:"request_id"`
}

// SearchRequest encapsulates all query parameters for the search API.
type SearchRequest struct {
	TenantID  string    `json:"tenant_id"  validate:"required"`
	Query     string    `json:"query"`
	Regex     string    `json:"regex"`
	Level     string    `json:"level"`
	Service   string    `json:"service"`
	Namespace string    `json:"namespace"`
	PodName   string    `json:"pod_name"`
	TraceID   string    `json:"trace_id"`
	StartTime time.Time `json:"start_time" validate:"required"`
	EndTime   time.Time `json:"end_time"   validate:"required"`
	Page      int       `json:"page"`
	PageSize  int       `json:"page_size"`
	OrderBy   string    `json:"order_by"`
	OrderDir  string    `json:"order_dir"`
}

// SearchResponse wraps paginated search results.
type SearchResponse struct {
	Logs      []LogEntry `json:"logs"`
	Total     uint64     `json:"total"`
	Page      int        `json:"page"`
	PageSize  int        `json:"page_size"`
	PageCount int        `json:"page_count"`
	Took      string     `json:"took"`
	Cached    bool       `json:"cached"`
}

// ErrorResponse is the standard API error envelope.
type ErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}

// Claims is the JWT claims payload.
type Claims struct {
	TenantID string   `json:"tenant_id"`
	UserID   string   `json:"user_id"`
	Roles    []string `json:"roles"`
	Exp      int64    `json:"exp"`
	Iat      int64    `json:"iat"`
}

// StreamFilter controls which logs flow to a WebSocket subscriber.
type StreamFilter struct {
	TenantID  string `json:"tenant_id"`
	Service   string `json:"service"`
	Namespace string `json:"namespace"`
	Level     string `json:"level"`
	Query     string `json:"query"`
}
