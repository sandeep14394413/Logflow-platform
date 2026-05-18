package hub

import (
	"testing"

	"go.uber.org/zap"
)

func newTestHub() *Hub {
	log, _ := zap.NewDevelopment()
	return New(log)
}

func TestMatchesFilter_TenantMismatch(t *testing.T) {
	f := StreamFilter{TenantID: "tenant-a"}
	msg := map[string]interface{}{"service": "api", "level": "INFO"}
	if matchesFilter(f, "tenant-b", msg) {
		t.Error("expected filter to reject different tenant")
	}
}

func TestMatchesFilter_TenantMatch(t *testing.T) {
	f := StreamFilter{TenantID: "tenant-a"}
	msg := map[string]interface{}{"service": "api", "level": "INFO"}
	if !matchesFilter(f, "tenant-a", msg) {
		t.Error("expected filter to pass same tenant with no other restrictions")
	}
}

func TestMatchesFilter_ServiceFilter(t *testing.T) {
	f := StreamFilter{TenantID: "t", Service: "api"}
	match := map[string]interface{}{"service": "api", "level": "INFO"}
	noMatch := map[string]interface{}{"service": "worker", "level": "INFO"}

	if !matchesFilter(f, "t", match) {
		t.Error("expected service filter to pass matching service")
	}
	if matchesFilter(f, "t", noMatch) {
		t.Error("expected service filter to reject non-matching service")
	}
}

func TestMatchesFilter_LevelFilter(t *testing.T) {
	f := StreamFilter{TenantID: "t", Level: "ERROR"}
	match := map[string]interface{}{"level": "ERROR", "message": "boom"}
	noMatch := map[string]interface{}{"level": "INFO", "message": "ok"}

	if !matchesFilter(f, "t", match) {
		t.Error("expected level filter to pass ERROR")
	}
	if matchesFilter(f, "t", noMatch) {
		t.Error("expected level filter to reject INFO")
	}
}

func TestMatchesFilter_NamespaceFilter(t *testing.T) {
	f := StreamFilter{TenantID: "t", Namespace: "production"}
	match := map[string]interface{}{"namespace": "production"}
	noMatch := map[string]interface{}{"namespace": "staging"}

	if !matchesFilter(f, "t", match) {
		t.Error("expected namespace filter to match 'production'")
	}
	if matchesFilter(f, "t", noMatch) {
		t.Error("expected namespace filter to reject 'staging'")
	}
}

func TestMatchesFilter_QuerySubstring(t *testing.T) {
	f := StreamFilter{TenantID: "t", Query: "timeout"}
	match := map[string]interface{}{"message": "connection timeout after 5s"}
	noMatch := map[string]interface{}{"message": "all systems normal"}

	if !matchesFilter(f, "t", match) {
		t.Error("expected query to match substring 'timeout'")
	}
	if matchesFilter(f, "t", noMatch) {
		t.Error("expected query to reject message without 'timeout'")
	}
}

func TestMatchesFilter_AllFilters(t *testing.T) {
	f := StreamFilter{
		TenantID:  "t",
		Service:   "api",
		Namespace: "prod",
		Level:     "ERROR",
		Query:     "fail",
	}
	match := map[string]interface{}{
		"service":   "api",
		"namespace": "prod",
		"level":     "ERROR",
		"message":   "request failed",
	}
	if !matchesFilter(f, "t", match) {
		t.Error("expected all-filter match to pass")
	}
}

func TestHub_RegisterUnregister(t *testing.T) {
	h := newTestHub()
	if len(h.clients) != 0 {
		t.Error("expected empty hub initially")
	}

	cl := &client{
		id:   "test-client-1",
		send: make(chan []byte, 10),
		filter: StreamFilter{TenantID: "t"},
	}
	h.register(cl)
	if len(h.clients) != 1 {
		t.Errorf("expected 1 client after register, got %d", len(h.clients))
	}

	h.unregister(cl)
	if len(h.clients) != 0 {
		t.Errorf("expected 0 clients after unregister, got %d", len(h.clients))
	}
}

func TestHub_Broadcast_NoSubscribers(t *testing.T) {
	h := newTestHub()
	// Should not panic with no subscribers.
	h.Broadcast("tenant-x", []byte(`{"level":"INFO","message":"hello","service":"svc"}`))
}

func TestHub_Broadcast_MatchingSubscriber(t *testing.T) {
	h := newTestHub()
	ch := make(chan []byte, 5)
	cl := &client{
		id:   "client-1",
		send: ch,
		filter: StreamFilter{TenantID: "t", Level: "ERROR"},
	}
	h.register(cl)
	defer h.unregister(cl)

	payload := []byte(`{"level":"ERROR","service":"api","message":"crash","namespace":"prod"}`)
	h.Broadcast("t", payload)

	select {
	case msg := <-ch:
		if string(msg) != string(payload) {
			t.Errorf("expected payload %q, got %q", payload, msg)
		}
	default:
		t.Error("expected message to be delivered to matching subscriber")
	}
}

func TestHub_Broadcast_NonMatchingSubscriber(t *testing.T) {
	h := newTestHub()
	ch := make(chan []byte, 5)
	cl := &client{
		id:   "client-2",
		send: ch,
		filter: StreamFilter{TenantID: "t", Level: "ERROR"},
	}
	h.register(cl)
	defer h.unregister(cl)

	// INFO level — should NOT match the ERROR filter.
	h.Broadcast("t", []byte(`{"level":"INFO","service":"api","message":"ok"}`))

	select {
	case <-ch:
		t.Error("expected no message for non-matching filter")
	default:
		// Correct — nothing delivered.
	}
}

func TestHub_Broadcast_TenantIsolation(t *testing.T) {
	h := newTestHub()
	ch := make(chan []byte, 5)
	cl := &client{
		id:   "client-tenant-a",
		send: ch,
		filter: StreamFilter{TenantID: "tenant-a"},
	}
	h.register(cl)
	defer h.unregister(cl)

	// Broadcast for a different tenant — must NOT reach this client.
	h.Broadcast("tenant-b", []byte(`{"level":"INFO","message":"other tenant"}`))

	select {
	case <-ch:
		t.Error("expected tenant isolation — message should not cross tenant boundaries")
	default:
		// Correct.
	}
}

func TestGetString(t *testing.T) {
	m := map[string]interface{}{
		"level":   "INFO",
		"missing": nil,
	}
	if getString(m, "level") != "INFO" {
		t.Error("expected 'INFO'")
	}
	if getString(m, "nonexistent") != "" {
		t.Error("expected empty string for missing key")
	}
	if getString(m, "missing") != "" {
		t.Error("expected empty string for nil value")
	}
}
