// Package hub implements a WebSocket fan-out hub for real-time log streaming.
// Kafka consumers push log events; the hub broadcasts to matching subscribers.
package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	wsConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "logflow",
		Subsystem: "websocket",
		Name:      "active_connections",
		Help:      "Active WebSocket connections.",
	}, []string{"tenant"})

	wsMessagesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "logflow",
		Subsystem: "websocket",
		Name:      "messages_sent_total",
		Help:      "Total log messages sent to WebSocket clients.",
	}, []string{"tenant"})

	wsBroadcastDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "logflow",
		Subsystem: "websocket",
		Name:      "broadcast_duration_seconds",
		Help:      "Time to broadcast a message to all matching subscribers.",
		Buckets:   []float64{.0001, .0005, .001, .005, .01, .025, .05},
	})
)

// StreamFilter controls which logs are forwarded to a subscriber.
type StreamFilter struct {
	TenantID  string `json:"tenant_id"`
	Service   string `json:"service"`
	Namespace string `json:"namespace"`
	Level     string `json:"level"`
	Query     string `json:"query"`
}

// client is a single WebSocket subscriber.
type client struct {
	id     string
	conn   *websocket.Conn
	send   chan []byte
	filter StreamFilter
	mu     sync.Mutex
}

// Hub manages all connected WebSocket clients and fan-out.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*client
	log     *zap.Logger
}

// New creates and returns an initialised Hub.
func New(log *zap.Logger) *Hub {
	return &Hub{
		clients: make(map[string]*client, 256),
		log:     log,
	}
}

// Broadcast fans out a raw log JSON payload to all clients whose filter matches.
// This is called from the Kafka consumer goroutine and must be non-blocking.
func (h *Hub) Broadcast(tenantID string, payload []byte) {
	start := time.Now()
	h.mu.RLock()
	defer h.mu.RUnlock()

	var logMsg map[string]interface{}
	_ = json.Unmarshal(payload, &logMsg)

	for _, c := range h.clients {
		if !matchesFilter(c.filter, tenantID, logMsg) {
			continue
		}
		select {
		case c.send <- payload:
			wsMessagesSent.WithLabelValues(tenantID).Inc()
		default:
			// Client send buffer full — skip to avoid blocking the broadcaster.
			h.log.Warn("ws client buffer full, skipping", zap.String("client", c.id))
		}
	}
	wsBroadcastDuration.Observe(time.Since(start).Seconds())
}

var upgrader = websocket.Upgrader{
	HandshakeTimeout: 10 * time.Second,
	ReadBufferSize:   4096,
	WriteBufferSize:  32768,
	CheckOrigin: func(r *http.Request) bool {
		// Origin check is handled by the API gateway; trust inbound here.
		return true
	},
}

// Handle upgrades an HTTP connection to WebSocket and manages the client lifecycle.
func (h *Hub) Handle(c *gin.Context) {
	tenantID := c.GetHeader("X-Tenant-ID")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": "BAD_REQUEST", "message": "X-Tenant-ID required"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.log.Error("ws upgrade failed", zap.Error(err))
		return
	}

	// The client sends a StreamFilter JSON immediately after connecting.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var filter StreamFilter
	if err := conn.ReadJSON(&filter); err != nil {
		h.log.Warn("ws filter read failed", zap.Error(err))
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{}) // Reset after initial handshake.
	filter.TenantID = tenantID        // Enforce tenant from JWT — never trust payload.

	cl := &client{
		id:     c.GetHeader("X-Request-ID"),
		conn:   conn,
		send:   make(chan []byte, 256),
		filter: filter,
	}

	h.register(cl)
	wsConnections.WithLabelValues(tenantID).Inc()
	h.log.Info("ws client connected", zap.String("tenant", tenantID), zap.String("client", cl.id))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)

	// Writer goroutine: drain the send channel.
	go func() {
		defer wg.Done()
		defer cancel()
		conn.SetWriteDeadline(time.Time{})
		pingTicker := time.NewTicker(30 * time.Second)
		defer pingTicker.Stop()

		for {
			select {
			case msg, ok := <-cl.send:
				if !ok {
					return
				}
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					h.log.Warn("ws write error", zap.Error(err))
					return
				}
			case <-pingTicker.C:
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Reader goroutine: detect disconnects and honour filter updates.
	go func() {
		defer wg.Done()
		defer cancel()
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					h.log.Warn("ws read error", zap.Error(err))
				}
				return
			}
			// Support runtime filter updates (client sends new JSON).
			var newFilter StreamFilter
			if json.Unmarshal(msg, &newFilter) == nil {
				newFilter.TenantID = tenantID
				cl.mu.Lock()
				cl.filter = newFilter
				cl.mu.Unlock()
				h.log.Debug("ws filter updated", zap.String("client", cl.id))
			}
		}
	}()

	wg.Wait()
	h.unregister(cl)
	conn.Close()
	wsConnections.WithLabelValues(tenantID).Dec()
	h.log.Info("ws client disconnected", zap.String("tenant", tenantID), zap.String("client", cl.id))
}

func (h *Hub) register(c *client) {
	h.mu.Lock()
	h.clients[c.id] = c
	h.mu.Unlock()
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	delete(h.clients, c.id)
	h.mu.Unlock()
	close(c.send)
}

// matchesFilter returns true when the log matches the subscriber's subscription.
func matchesFilter(f StreamFilter, tenantID string, msg map[string]interface{}) bool {
	if f.TenantID != tenantID {
		return false
	}
	if f.Service != "" && getString(msg, "service") != f.Service {
		return false
	}
	if f.Namespace != "" && getString(msg, "namespace") != f.Namespace {
		return false
	}
	if f.Level != "" && getString(msg, "level") != f.Level {
		return false
	}
	if f.Query != "" && !strings.Contains(getString(msg, "message"), f.Query) {
		return false
	}
	return true
}

func getString(m map[string]interface{}, key string) string {
	v, _ := m[key].(string)
	return v
}
