package handler

import (
	"testing"
	"time"
)

func TestBuildCacheKey_Deterministic(t *testing.T) {
	req := SearchRequest{
		Query:     "error",
		Level:     "ERROR",
		Service:   "api",
		Namespace: "prod",
		StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
		Page:      1,
		PageSize:  100,
		OrderDir:  "desc",
	}
	k1 := buildCacheKey("tenant-a", req)
	k2 := buildCacheKey("tenant-a", req)
	if k1 != k2 {
		t.Errorf("cache key should be deterministic: %q != %q", k1, k2)
	}
}

func TestBuildCacheKey_TenantIsolation(t *testing.T) {
	req := SearchRequest{
		Query:     "error",
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now(),
		Page:      1,
		PageSize:  100,
	}
	k1 := buildCacheKey("tenant-a", req)
	k2 := buildCacheKey("tenant-b", req)
	if k1 == k2 {
		t.Error("different tenants must produce different cache keys")
	}
}

func TestBuildCacheKey_DifferentQueries(t *testing.T) {
	base := SearchRequest{
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now(),
		Page:      1,
		PageSize:  100,
	}
	r1 := base
	r1.Query = "timeout"
	r2 := base
	r2.Query = "connection refused"

	k1 := buildCacheKey("t", r1)
	k2 := buildCacheKey("t", r2)
	if k1 == k2 {
		t.Error("different queries must produce different cache keys")
	}
}

func TestBuildCacheKey_HasPrefix(t *testing.T) {
	req := SearchRequest{
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now(),
	}
	key := buildCacheKey("myTenant", req)
	if len(key) == 0 {
		t.Error("cache key must not be empty")
	}
	// Key must be scoped to tenant for Redis keyspace isolation.
	if key[:7+len("myTenant")] != "search:myTenant" {
		t.Errorf("expected key to start with 'search:myTenant', got %q", key[:30])
	}
}

func TestBuildQuery_BasicFilters(t *testing.T) {
	req := SearchRequest{
		Level:     "ERROR",
		Service:   "api-server",
		Namespace: "production",
		StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		Page:      1,
		PageSize:  50,
		OrderDir:  "desc",
	}
	query, countQuery, args := buildQuery("tenant-x", req)

	if query == "" {
		t.Error("expected non-empty query")
	}
	if countQuery == "" {
		t.Error("expected non-empty count query")
	}
	// tenant_id + timestamp range + level + service + namespace = 5 args minimum
	if len(args) < 5 {
		t.Errorf("expected at least 5 args, got %d", len(args))
	}
}

func TestBuildQuery_FullTextSearch(t *testing.T) {
	req := SearchRequest{
		Query:     "timeout connection",
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now(),
		Page:      1,
		PageSize:  100,
	}
	query, _, args := buildQuery("t", req)

	// "timeout connection" → 2 hasToken calls → 2 extra args
	// baseline args: tenant_id, start_time, end_time = 3
	// + 2 tokens = 5
	if len(args) < 5 {
		t.Errorf("expected hasToken args for full-text query, got %d args", len(args))
	}
	_ = query
}

func TestBuildQuery_RegexFilter(t *testing.T) {
	req := SearchRequest{
		Regex:     "error.*timeout",
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now(),
		Page:      1,
		PageSize:  100,
	}
	_, _, args := buildQuery("t", req)
	// tenant + start + end + regex = 4
	if len(args) < 4 {
		t.Errorf("expected regex arg included, got %d args", len(args))
	}
}

func TestBuildQuery_AscOrder(t *testing.T) {
	req := SearchRequest{
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now(),
		Page:      1,
		PageSize:  10,
		OrderDir:  "asc",
	}
	query, _, _ := buildQuery("t", req)
	// ASC ordering must appear in the query string.
	found := false
	for i := 0; i < len(query)-2; i++ {
		if query[i:i+3] == "ASC" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected ASC in query for asc order_dir")
	}
}

func TestBuildQuery_Pagination(t *testing.T) {
	req := SearchRequest{
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now(),
		Page:      3,
		PageSize:  25,
		OrderDir:  "desc",
	}
	query, _, _ := buildQuery("t", req)
	if query == "" {
		t.Error("expected non-empty query for page 3")
	}
	// OFFSET should be 50 (page=3, size=25 → (3-1)*25=50)
	// Just ensure query is built without panic.
}
