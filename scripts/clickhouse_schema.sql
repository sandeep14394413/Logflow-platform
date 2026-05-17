-- ============================================================
-- LogFlow ClickHouse Schema
-- Engine: ReplicatedMergeTree (or MergeTree for single-node dev)
-- Designed for 1M+ writes/sec, fast range + full-text queries
-- ============================================================

-- ─── Database ────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS logflow
    COMMENT 'Log aggregation platform database';

-- ─── Distributed table (entry point for inserts/queries) ────
-- In a cluster, this sits in front of the sharded local tables.
-- For single-node dev, replace with plain MergeTree below.
CREATE TABLE IF NOT EXISTS logflow.logs_distributed ON CLUSTER '{cluster}'
(
    -- Identity
    id           UUID          DEFAULT generateUUIDv4() CODEC(ZSTD(1)),
    tenant_id    LowCardinality(String)                 CODEC(ZSTD(3)),

    -- Temporal (primary sort key dimension)
    timestamp    DateTime64(3, 'UTC')                   CODEC(Delta, ZSTD(1)),

    -- Kubernetes metadata (low cardinality — benefit from LowCardinality)
    namespace    LowCardinality(String)                 CODEC(ZSTD(3)),
    service      LowCardinality(String)                 CODEC(ZSTD(3)),
    pod_name     String                                 CODEC(ZSTD(1)),
    node_name    LowCardinality(String)                 CODEC(ZSTD(3)),
    environment  LowCardinality(String)                 CODEC(ZSTD(3)),
    level        LowCardinality(String)                 CODEC(ZSTD(3)),

    -- Tracing correlation
    trace_id     String                                 CODEC(ZSTD(1)),
    span_id      String                                 CODEC(ZSTD(1)),

    -- Payload
    message      String                                 CODEC(ZSTD(3)),
    host_ip      IPv4,

    -- Semi-structured maps stored as parallel arrays (fast key lookup)
    labels       Map(LowCardinality(String), String)    CODEC(ZSTD(3)),
    attributes   Map(LowCardinality(String), String)    CODEC(ZSTD(3)),

    -- Ingestion bookkeeping
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1))
)
ENGINE = Distributed('{cluster}', 'logflow', 'logs_local', xxHash32(tenant_id));

-- ─── Local (per-shard) table ─────────────────────────────────
CREATE TABLE IF NOT EXISTS logflow.logs_local ON CLUSTER '{cluster}'
(
    id           UUID          DEFAULT generateUUIDv4() CODEC(ZSTD(1)),
    tenant_id    LowCardinality(String)                 CODEC(ZSTD(3)),
    timestamp    DateTime64(3, 'UTC')                   CODEC(Delta, ZSTD(1)),
    namespace    LowCardinality(String)                 CODEC(ZSTD(3)),
    service      LowCardinality(String)                 CODEC(ZSTD(3)),
    pod_name     String                                 CODEC(ZSTD(1)),
    node_name    LowCardinality(String)                 CODEC(ZSTD(3)),
    environment  LowCardinality(String)                 CODEC(ZSTD(3)),
    level        LowCardinality(String)                 CODEC(ZSTD(3)),
    trace_id     String                                 CODEC(ZSTD(1)),
    span_id      String                                 CODEC(ZSTD(1)),
    message      String                                 CODEC(ZSTD(3)),
    host_ip      IPv4,
    labels       Map(LowCardinality(String), String)    CODEC(ZSTD(3)),
    attributes   Map(LowCardinality(String), String)    CODEC(ZSTD(3)),
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1))
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/logflow/logs_local',
    '{replica}'
)
-- ── Partition: one part per day — balances compaction vs. query scan
PARTITION BY toYYYYMMDD(timestamp)
-- ── Primary key: drives the sparse index and sort order
-- tenant_id first forces tenant isolation at the storage level
PRIMARY KEY (tenant_id, toStartOfHour(timestamp), service)
-- ── Wider sort key: enables efficient filtering without full scan
ORDER BY (tenant_id, toStartOfHour(timestamp), service, namespace, level, timestamp)
-- ── TTL: auto-delete data older than 30 days; archive to S3 after 7 days
TTL
    toDateTime(timestamp) + INTERVAL 30 DAY DELETE,
    toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold'
-- ── Storage policy: hot NVMe → cold S3
SETTINGS
    storage_policy = 'hot_cold',
    index_granularity = 8192,
    merge_with_ttl_timeout = 3600,
    min_bytes_for_wide_part = 10485760;  -- 10 MB

-- ─── Skipping indices (bloom filter for full-text search) ───
ALTER TABLE logflow.logs_local ON CLUSTER '{cluster}'
    ADD INDEX idx_message message TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4,
    ADD INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    ADD INDEX idx_pod_name pod_name TYPE bloom_filter(0.01) GRANULARITY 1;

-- ─── Materialised view: per-minute log counts (for dashboards) ──
CREATE MATERIALIZED VIEW IF NOT EXISTS logflow.logs_counts_mv
ON CLUSTER '{cluster}'
TO logflow.logs_counts
AS
SELECT
    tenant_id,
    service,
    namespace,
    level,
    toStartOfMinute(timestamp) AS minute,
    count() AS cnt
FROM logflow.logs_local
GROUP BY tenant_id, service, namespace, level, minute;

CREATE TABLE IF NOT EXISTS logflow.logs_counts ON CLUSTER '{cluster}'
(
    tenant_id  LowCardinality(String),
    service    LowCardinality(String),
    namespace  LowCardinality(String),
    level      LowCardinality(String),
    minute     DateTime,
    cnt        UInt64
)
ENGINE = ReplicatedSummingMergeTree(
    '/clickhouse/tables/{shard}/logflow/logs_counts',
    '{replica}',
    cnt
)
ORDER BY (tenant_id, service, namespace, level, minute)
TTL minute + INTERVAL 90 DAY DELETE;

-- ─── Users & permissions (tenant isolation) ─────────────────
CREATE USER IF NOT EXISTS logflow_writer
    IDENTIFIED WITH sha256_password BY '{WRITER_PASSWORD}'
    HOST REGEXP '.*\.logflow\.svc\.cluster\.local';

CREATE USER IF NOT EXISTS logflow_reader
    IDENTIFIED WITH sha256_password BY '{READER_PASSWORD}'
    HOST REGEXP '.*\.logflow\.svc\.cluster\.local';

GRANT INSERT ON logflow.logs_distributed TO logflow_writer;
GRANT INSERT ON logflow.logs_counts TO logflow_writer;

GRANT SELECT ON logflow.logs_distributed TO logflow_reader;
GRANT SELECT ON logflow.logs_counts TO logflow_reader;
GRANT SELECT ON logflow.logs_counts_mv TO logflow_reader;

-- ─── Dev / single-node variant (no cluster macro needed) ────
-- Uncomment below when running outside a cluster:
/*
CREATE TABLE IF NOT EXISTS logflow.logs
(
    id           UUID          DEFAULT generateUUIDv4(),
    tenant_id    LowCardinality(String),
    timestamp    DateTime64(3, 'UTC'),
    namespace    LowCardinality(String),
    service      LowCardinality(String),
    pod_name     String,
    node_name    LowCardinality(String),
    environment  LowCardinality(String),
    level        LowCardinality(String),
    trace_id     String,
    span_id      String,
    message      String,
    host_ip      IPv4,
    labels       Map(LowCardinality(String), String),
    attributes   Map(LowCardinality(String), String),
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
PRIMARY KEY (tenant_id, toStartOfHour(timestamp), service)
ORDER BY (tenant_id, toStartOfHour(timestamp), service, namespace, level, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;
*/
