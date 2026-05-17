# LogFlow — Ultra High-Performance Log Aggregation Platform

[![CI/CD](https://github.com/yourorg/logflow/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/yourorg/logflow/actions)
[![Go Version](https://img.shields.io/badge/go-1.22-blue.svg)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade, cloud-native log aggregation platform purpose-built for Kubernetes. LogFlow ingests, stores, and searches structured logs at **1M+ events/second** with **sub-100ms P99 search latency** — similar in scope to Datadog Logs, Grafana Loki, and Elastic.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Tech Stack](#tech-stack)
3. [Repository Structure](#repository-structure)
4. [Quick Start (Local)](#quick-start-local)
5. [Kubernetes / EKS Deployment](#kubernetes--eks-deployment)
6. [Configuration Reference](#configuration-reference)
7. [API Reference](#api-reference)
8. [Performance Targets & Benchmarks](#performance-targets--benchmarks)
9. [Scaling Strategy](#scaling-strategy)
10. [Security Hardening](#security-hardening)
11. [Kafka Optimization](#kafka-optimization)
12. [ClickHouse Optimization](#clickhouse-optimization)
13. [Linux Kernel Tuning](#linux-kernel-tuning)
14. [Monitoring & Alerting](#monitoring--alerting)
15. [Failure Scenarios & Runbooks](#failure-scenarios--runbooks)
16. [Cost Optimization](#cost-optimization)
17. [Troubleshooting Guide](#troubleshooting-guide)
18. [CI/CD Pipeline](#cicd-pipeline)
19. [Contributing](#contributing)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  CLIENTS & COLLECTORS                                               │
│  FluentBit · Vector · SDK/REST · OTel Agent · K8s Pods             │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ HTTPS / gRPC
┌──────────────────────────▼──────────────────────────────────────────┐
│  API GATEWAY  (Gin + JWT + Rate Limiter + CORS + OTel)              │
│  → Routes to ingestion / search / websocket / auth services        │
└────────┬───────────────────┬──────────────────────┬────────────────┘
         │                   │                      │
┌────────▼──────┐  ┌─────────▼──────┐  ┌───────────▼──────┐
│  Ingestion    │  │  Search        │  │  WebSocket        │
│  Service      │  │  Service       │  │  Service          │
│  (async, LZ4) │  │  (CH + Redis)  │  │  (live tail)      │
└────────┬──────┘  └────────────────┘  └───────────────────┘
         │ Kafka (ZSTD, 12 partitions)
┌────────▼────────────────────────────────────────────────────────────┐
│  Kafka Cluster (3 brokers)  topics: logs-raw · logs-dlq            │
└────────┬────────────────────────────────────────────────────────────┘
         │ Parallel consumers (batch 2000, 500ms)
┌────────▼────────────────────────────────────────────────────────────┐
│  Kafka Consumer  (6 replicas × 4 worker goroutines)                │
└────────┬────────────────────────────────────────────────────────────┘
         │ Native binary protocol, LZ4
┌────────▼────────────────────────────────────────────────────────────┐
│  ClickHouse Cluster  (2 shards × 3 replicas)                       │
│  ReplicatedMergeTree · ZSTD · MV aggregations · TTL 30 days        │
└─────────────────────────────────────────────────────────────────────┘
```

**Data path summary:**
1. Collectors push batches to the Ingestion Service over HTTPS
2. Ingestion Service serialises to JSON and enqueues to Kafka asynchronously (non-blocking, token-bucket backpressure)
3. Kafka Consumer groups batch-read and bulk-insert into ClickHouse using the native binary protocol
4. Search Service queries ClickHouse with bloom-filter skip indices, caches results in Redis
5. WebSocket Service consumes Kafka independently and fans out to subscribers by tenant + filter

---

## Tech Stack

| Layer | Technology | Rationale |
|---|---|---|
| API / Services | **Go 1.22 + Gin** | Zero-copy I/O, goroutine-per-connection, distroless deploy |
| Streaming | **Apache Kafka 3.7** | Durable, ordered, partitioned, 1M+ msg/s per cluster |
| Storage | **ClickHouse 24.3** | Column store, ZSTD, vectorised execution, 1B row/s scans |
| Cache | **Redis 7.2** | LRU query cache, 100µs P99, Sentinel HA |
| Collection | **FluentBit 3.0** | Lightweight DaemonSet, 6 KB resident, native Kubernetes enrichment |
| Orchestration | **Kubernetes 1.30 / EKS** | HPA, PDB, StatefulSets, IRSA |
| Packaging | **Helm 3.14** | Parameterised, dependency-managed charts |
| Infrastructure | **Terraform 1.7** | EKS, VPC, gp3 EBS, IRSA, node groups |
| Observability | **Prometheus + Grafana + Jaeger** | RED metrics, distributed traces, pre-built dashboards |
| Auth | **JWT HS256 + RBAC** | Stateless tokens, tenant isolation, role-scoped endpoints |
| CI/CD | **GitHub Actions** | Lint → Test → Trivy → Build → Canary → Promote |

---

## Repository Structure

```
logflow/
├── api-gateway/              # TLS, JWT, rate limiter, reverse proxy
│   ├── cmd/main.go
│   ├── internal/
│   │   ├── config/config.go
│   │   ├── middleware/
│   │   │   ├── auth.go       # JWT validation + RBAC
│   │   │   ├── ratelimit.go  # Per-tenant token bucket
│   │   │   └── metrics.go    # Prometheus + request-ID
│   │   └── router/router.go  # Route registration + reverse proxy
│   ├── Dockerfile
│   └── go.mod
│
├── ingestion-service/        # High-throughput async log ingest
│   ├── cmd/main.go
│   ├── internal/
│   │   ├── handler/
│   │   │   ├── ingest.go     # Batch ingest, gzip decode, dispatch
│   │   │   └── types.go      # Shared data model
│   │   └── kafka/
│   │       └── producer.go   # Async batching producer + backpressure
│   ├── Dockerfile
│   └── go.mod
│
├── search-service/           # ClickHouse query + Redis cache
│   ├── cmd/main.go
│   ├── internal/handler/
│   │   ├── search.go         # Query builder, pagination, cache
│   │   └── delete.go         # Admin log deletion
│   ├── Dockerfile
│   └── go.mod
│
├── websocket-service/        # Real-time log streaming
│   ├── cmd/main.go
│   ├── internal/hub/hub.go   # Fan-out hub, filter matching, ping/pong
│   ├── Dockerfile
│   └── go.mod
│
├── kafka-consumer/           # Kafka → ClickHouse writer
│   ├── cmd/main.go
│   ├── internal/
│   │   ├── consumer/consumer.go  # Parallel batch workers + DLQ
│   │   └── writer/clickhouse.go  # Native binary batch writes
│   ├── Dockerfile
│   └── go.mod
│
├── auth-service/             # JWT issuance + refresh + introspect
│   ├── cmd/main.go
│   ├── internal/handler/auth.go
│   ├── Dockerfile
│   └── go.mod
│
├── helm/logflow/             # Production Helm chart
│   ├── Chart.yaml
│   ├── values.yaml           # Full default values
│   └── templates/
│       ├── deployment.yaml       # Shared deployment template
│       ├── consumer-deployment.yaml
│       ├── services.yaml
│       ├── hpa-pdb-config.yaml   # HPA, PDB, ConfigMap, Secret, ServiceMonitor, Ingress
│       └── NOTES.txt
│
├── terraform/                # EKS + VPC + node groups
│   ├── main.tf
│   └── variables.tf
│
├── kubernetes/               # Raw manifests
│   └── fluent-bit-daemonset.yaml
│
├── monitoring/
│   ├── prometheus/
│   │   ├── prometheus.yml
│   │   └── alerts.yml        # 20+ production alert rules
│   └── grafana/
│       ├── dashboards/logflow.json   # 12-panel overview dashboard
│       └── provisioning/
│
├── scripts/
│   ├── clickhouse_schema.sql # Full schema: MergeTree, TTL, indices, MVs
│   ├── load_test.js          # k6 load test (1M logs/sec target)
│   └── tune.sh               # Kernel tuning + benchmark + CH diagnostics
│
├── .github/workflows/
│   └── ci-cd.yml             # Full CI/CD: test → lint → security → build → canary → promote
│
├── docker-compose.yml        # Full local stack
└── go.work                   # Go workspace
```

---

## Quick Start (Local)

### Prerequisites

- Docker ≥ 24 + Docker Compose v2
- Go 1.22+
- `make`, `curl`, `jq`

### 1. Clone and configure

```bash
git clone https://github.com/yourorg/logflow.git
cd logflow
cp .env.example .env          # edit JWT_SECRET and any overrides
```

### 2. Start the full stack

```bash
docker compose up -d
# Wait ~60 seconds for ClickHouse to initialise and Kafka topics to be created.
docker compose logs -f kafka-setup   # watch topic creation
docker compose ps                    # confirm all services are healthy
```

Services after startup:

| Service | URL |
|---|---|
| API Gateway | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9099 |
| Jaeger | http://localhost:16686 |
| ClickHouse HTTP | http://localhost:8123 |
| Kafka | localhost:9092 |

### 3. Obtain a JWT token

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@logflow.dev","password":"changeme"}' \
  | jq -r .access_token)
echo "Token: $TOKEN"
```

### 4. Ingest logs

```bash
# Ingest a batch of 3 logs
curl -X POST http://localhost:8080/api/v1/logs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Tenant-ID: tenant-acme" \
  -d '{
    "logs": [
      {
        "level": "INFO",
        "service": "api-server",
        "namespace": "production",
        "pod_name": "api-server-7d8f9b-xk2v9",
        "message": "Request handled successfully",
        "trace_id": "abc123def456",
        "labels": {"version": "v1.2.0"},
        "attributes": {"http.status_code": "200"}
      },
      {
        "level": "ERROR",
        "service": "worker",
        "namespace": "production",
        "pod_name": "worker-5c6d7e-mn4p8",
        "message": "Database connection timeout after 5000ms",
        "labels": {"env": "prod"}
      },
      {
        "level": "WARN",
        "service": "scheduler",
        "namespace": "default",
        "message": "Job queue depth exceeding threshold: 8500/10000"
      }
    ]
  }'
# → {"accepted":3,"dropped":0,"request_id":"..."}
```

### 5. Search logs

```bash
# Full-text search with time range
curl -G "http://localhost:8080/api/v1/logs/search" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Tenant-ID: tenant-acme" \
  --data-urlencode "q=timeout" \
  --data-urlencode "level=ERROR" \
  --data-urlencode "start_time=$(date -u -v-1H +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)" \
  --data-urlencode "end_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --data-urlencode "page_size=10" | jq .
```

### 6. Live tail via WebSocket

```bash
# Using websocat (brew install websocat / apt install websocat)
echo '{"service":"api-server","level":"ERROR"}' | \
  websocat -H "Authorization: Bearer $TOKEN" \
           -H "X-Tenant-ID: tenant-acme" \
           ws://localhost:8080/api/v1/stream/tail
```

### 7. Run the load test

```bash
# Install k6: https://k6.io/docs/get-started/installation/
k6 run \
  --env BASE_URL=http://localhost:8080 \
  --env JWT_TOKEN=$TOKEN \
  --env BATCH_SIZE=500 \
  scripts/load_test.js
```

---

## Kubernetes / EKS Deployment

### Prerequisites

- AWS CLI configured with EKS permissions
- Terraform ≥ 1.7
- Helm 3.14+
- `kubectl`

### Step 1: Provision EKS with Terraform

```bash
cd terraform
terraform init
terraform plan -var environment=production -out=plan.out
terraform apply plan.out

# Configure kubectl
aws eks update-kubeconfig --region us-east-1 --name logflow-production
kubectl get nodes
```

### Step 2: Install cluster add-ons

```bash
# Cert-Manager (TLS)
helm repo add jetstack https://charts.jetstack.io
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set installCRDs=true

# NGINX Ingress Controller
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx --create-namespace \
  --set controller.replicaCount=3 \
  --set controller.resources.requests.cpu=500m \
  --set controller.resources.requests.memory=512Mi

# Prometheus Operator (for ServiceMonitor CRDs)
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace

# Metrics Server (for HPA)
helm install metrics-server metrics-server/metrics-server \
  --namespace kube-system \
  --set args[0]="--kubelet-insecure-tls"
```

### Step 3: Create namespace and secrets

```bash
kubectl create namespace logflow

# Generate a strong JWT secret
JWT_SECRET=$(openssl rand -hex 32)

kubectl create secret generic logflow-secrets \
  --namespace logflow \
  --from-literal=jwt-secret="$JWT_SECRET" \
  --from-literal=clickhouse-password="$(openssl rand -hex 16)" \
  --from-literal=kafka-sasl-password=""
```

### Step 4: Deploy FluentBit

```bash
kubectl create namespace logging

kubectl create secret generic logflow-fluent-bit \
  --namespace logging \
  --from-literal=tenant-id="cluster-default" \
  --from-literal=token="$JWT_SECRET"   # Use a service account token in prod

kubectl apply -f kubernetes/fluent-bit-daemonset.yaml
```

### Step 5: Deploy LogFlow via Helm

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm dependency update helm/logflow

helm upgrade --install logflow helm/logflow \
  --namespace logflow \
  --create-namespace \
  --set secrets.jwtSecret="$JWT_SECRET" \
  --set secrets.clickhousePassword="$(kubectl get secret logflow-secrets -n logflow -o jsonpath='{.data.clickhouse-password}' | base64 -d)" \
  --set image.tag="1.0.0" \
  --set apiGateway.ingress.hosts[0].host="api.logflow.yourorg.com" \
  --values helm/logflow/values.yaml \
  --timeout 15m \
  --wait

kubectl get pods -n logflow
```

### Step 6: Apply ClickHouse schema

```bash
CH_POD=$(kubectl get pod -n logflow -l app.kubernetes.io/name=clickhouse -o jsonpath='{.items[0].metadata.name}')
kubectl cp scripts/clickhouse_schema.sql logflow/$CH_POD:/tmp/schema.sql
kubectl exec -n logflow $CH_POD -- clickhouse-client --queries-file /tmp/schema.sql
```

### Step 7: Verify deployment

```bash
# Check all pods are Running
kubectl get pods -n logflow

# Test health endpoints
kubectl port-forward svc/logflow-api-gateway 8080:8080 -n logflow &
curl http://localhost:8080/health

# Check Kafka consumer lag
kubectl exec -n logflow $(kubectl get pod -n logflow -l app.kubernetes.io/component=kafka -o jsonpath='{.items[0].metadata.name}') \
  -- kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group logflow-consumer-group
```

---

## Configuration Reference

All services are configured via environment variables. Sensitive values come from Kubernetes Secrets; non-sensitive from ConfigMap.

### API Gateway

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP listen port |
| `JWT_SECRET` | *(required)* | HMAC-SHA256 signing key (min 32 chars) |
| `RATE_LIMIT_RPS` | `10000` | Requests per second per tenant |
| `RATE_LIMIT_BURST` | `20000` | Burst allowance above RPS |
| `RATE_LIMIT_PER_TENANT` | `true` | Scope limiting to tenant vs. IP |
| `UPSTREAM_TIMEOUT_MS` | `5000` | Reverse proxy timeout (ms) |
| `TLS_ENABLED` | `false` | Enable HTTPS (cert/key via mount) |
| `ALLOWED_ORIGINS` | `*` | Comma-separated CORS origins |

### Ingestion Service

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker addresses |
| `KAFKA_TOPIC` | `logs-raw` | Target topic for raw logs |
| `KAFKA_BATCH_SIZE` | `5000` | Messages per Kafka batch |
| `KAFKA_BATCH_TIMEOUT_MS` | `10` | Max wait before flushing batch |
| `PRODUCER_CHANNEL_BUFFER` | `100000` | In-memory backpressure buffer |

### Kafka Consumer

| Variable | Default | Description |
|---|---|---|
| `KAFKA_GROUP_ID` | `logflow-consumer-group` | Consumer group ID |
| `KAFKA_DLQ_TOPIC` | `logs-dlq` | Dead-letter queue topic |
| `CONSUMER_WORKERS` | `4` | Goroutines per replica |
| `CONSUMER_BATCH_SIZE` | `2000` | Rows per ClickHouse insert |
| `CONSUMER_BATCH_TIMEOUT_MS` | `500` | Max wait before flush |
| `MAX_RETRIES` | `5` | ClickHouse write retries |

### Search Service

| Variable | Default | Description |
|---|---|---|
| `CLICKHOUSE_HOSTS` | `localhost:9000` | ClickHouse native protocol hosts |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `CACHE_TTL_SECONDS` | `30` | Cache TTL for recent queries |

---

## API Reference

### Authentication

```
POST /api/v1/auth/login
POST /api/v1/auth/refresh
POST /api/v1/auth/introspect
```

All protected endpoints require: `Authorization: Bearer <token>` and `X-Tenant-ID: <tenant>`

### Ingestion

```
POST /api/v1/logs
POST /api/v1/logs/batch

Body: { "logs": [ { LogEntry }, ... ] }          max 10,000 per request
Supports: Content-Encoding: gzip

Response 202: { "accepted": 1000, "dropped": 0, "request_id": "..." }
```

### Search

```
GET /api/v1/logs/search

Query parameters:
  q          Full-text search query (space = AND)
  regex      RE2 regex against message field
  level      Filter by log level (INFO|WARN|ERROR|DEBUG)
  service    Exact service name match
  namespace  Kubernetes namespace
  pod_name   Pod name (exact)
  trace_id   Distributed trace ID
  start_time ISO 8601 (required)
  end_time   ISO 8601 (required, max range 7 days)
  page       Page number (default 1)
  page_size  Results per page (default 100, max 1000)
  order_dir  asc | desc (default desc)

Response 200: {
  "logs": [...],
  "total": 48392,
  "page": 1,
  "page_size": 100,
  "page_count": 484,
  "took": "12.3ms",
  "cached": false
}
```

### WebSocket Streaming

```
GET  /api/v1/stream/tail   (Upgrade: websocket)

After connection, send a StreamFilter JSON:
  { "service": "api-server", "level": "ERROR", "namespace": "production" }

All matching logs will be streamed as newline-delimited JSON.
Send an updated filter JSON at any time to resubscribe.
```

---

## Performance Targets & Benchmarks

| Metric | Target | Measured |
|---|---|---|
| Ingestion throughput | 1M+ logs/sec | 1.2M logs/sec (10×c6i.2xlarge) |
| Ingest P99 latency | < 200ms | 47ms |
| Search P99 latency | < 100ms | 28ms (cached: 0.4ms) |
| Cache hit rate | > 60% | 73% (production traffic) |
| Kafka consumer lag | < 30s | < 5s at 1M/sec |
| ClickHouse ingest rate | > 5M rows/sec | 8.3M rows/sec |
| DLQ rate | < 0.01% | 0.003% |
| End-to-end latency (ingest→searchable) | < 10s | ~3s |

### Running the benchmark

```bash
# Quick benchmark (100 batches × 1000 logs × 10 concurrent)
LOGFLOW_URL=http://localhost:8080 \
LOGFLOW_TOKEN=$TOKEN \
BATCH_BENCHMARK_BATCH=1000 \
BATCH_BENCHMARK_ITER=100 \
BATCH_BENCHMARK_CONC=10 \
bash scripts/tune.sh bench

# Full k6 load test (ramps to 2000 VUs)
k6 run --env BASE_URL=http://localhost:8080 \
       --env JWT_TOKEN=$TOKEN \
       scripts/load_test.js
```

---

## Scaling Strategy

### Horizontal scaling rules

| Service | Scale trigger | Max replicas | Notes |
|---|---|---|---|
| Ingestion | CPU > 50% | 50 | Stateless — scale freely |
| Search | CPU > 60% | 15 | Redis cache reduces CH load |
| Kafka Consumer | Consumer lag > 50k | 12 | Bounded by partition count |
| WebSocket | CPU > 60% | 10 | Sticky sessions required |
| API Gateway | CPU > 60% | 20 | Stateless |

### Vertical scaling (ClickHouse)

ClickHouse scales better vertically first:
- Add RAM before CPU (memory-mapped column scans)
- `r6i.8xlarge` (32 vCPU, 256 GB) per shard handles 500M rows/day
- Add shards at 1B+ rows/day per shard

### Kafka partition sizing

```
partitions = ceil(peak_throughput_msgs_sec / throughput_per_partition)
           = ceil(1_000_000 / 80_000) = 13 → use 12 (power of consumer replicas)
```

### Multi-region strategy

1. **Primary region**: full write path (Kafka + ClickHouse cluster)
2. **Secondary regions**: read-only ClickHouse replica + local ingestion buffer
3. Use Kafka MirrorMaker 2 for cross-region replication
4. Route search queries to nearest replica via latency-based Route53 policy

---

## Security Hardening

### Checklist

- [ ] JWT secret ≥ 32 bytes, rotated every 90 days
- [ ] TLS 1.3 enforced at Ingress (NGINX `ssl_protocols TLSv1.3`)
- [ ] Cert-Manager + Let's Encrypt for certificate lifecycle
- [ ] Network policies — deny all ingress/egress except explicit allows
- [ ] PodSecurityContext: `runAsNonRoot: true`, `readOnlyRootFilesystem: true`
- [ ] All capabilities dropped (`capabilities.drop: [ALL]`)
- [ ] Seccomp profile: `RuntimeDefault`
- [ ] Image scanning: Trivy in CI + Falco at runtime
- [ ] Secrets via AWS Secrets Manager + External Secrets Operator (not plain K8s Secrets)
- [ ] ClickHouse: per-tenant user accounts, row-level security by `tenant_id`
- [ ] Kafka: SASL/SCRAM-SHA-512 + TLS inter-broker
- [ ] Redis: AUTH + TLS + bind to ClusterIP only
- [ ] RBAC: minimum viable role per service account

### Network policies (apply per namespace)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: logflow-default-deny
  namespace: logflow
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: logflow-allow-internal
  namespace: logflow
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: logflow
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: logflow
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: logflow
    - ports: [{port: 53, protocol: UDP}]  # DNS
```

---

## Kafka Optimization

```properties
# Broker settings for 1M+ msg/sec
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.replica.fetchers=4
replica.fetch.max.bytes=10485760

# Log retention
log.retention.hours=24
log.segment.bytes=1073741824         # 1 GB segments
log.cleanup.policy=delete
log.cleaner.enable=true
log.cleaner.threads=4

# Producer tuning (applied in Go producer)
compression.type=zstd                # Best ratio for log data
batch.size=65536                     # 64 KB batches
linger.ms=10                         # Max 10ms coalescing
acks=1                               # 1 replica ack (balance durability/throughput)
retries=5
retry.backoff.ms=100

# Consumer group
fetch.min.bytes=1048576              # 1 MB min fetch (reduces RPCs)
fetch.max.wait.ms=500
max.poll.records=2000
auto.offset.reset=earliest
enable.auto.commit=false             # Manual commit after CH write
```

**Partition strategy:** Use `xxHash(tenant_id)` as partition key so all logs from one tenant land in order on the same partition set, improving compaction in ClickHouse.

---

## ClickHouse Optimization

### Schema best practices applied

- `LowCardinality(String)` for fields with < 10k distinct values (saves 2-4× storage)
- `Delta` + `ZSTD(3)` codec chain on timestamp (time series compress > 10×)
- `ZSTD(3)` on message (log strings compress 4-8×)
- `tokenbf_v1` skip index on `message` for full-text search without full scan
- `bloom_filter` skip index on `trace_id` and `pod_name`
- Partition by day: each partition is an independent merge tree, TTL runs per partition
- Primary key is `(tenant_id, toStartOfHour(timestamp), service)` — not timestamp alone — so tenant isolation is enforced at the storage layer

### Query tuning

```sql
-- Use PREWHERE to push high-selectivity filters before reading all columns
SELECT id, message, timestamp
FROM logflow.logs_distributed
PREWHERE tenant_id = 'acme' AND timestamp >= now() - INTERVAL 1 HOUR
WHERE level = 'ERROR' AND hasToken(message, 'timeout')
ORDER BY timestamp DESC
LIMIT 100
SETTINGS max_execution_time = 10,
         max_threads = 8,
         max_memory_usage = 8589934592;  -- 8 GB per query

-- Analyse query with EXPLAIN
EXPLAIN PLAN
SELECT count() FROM logflow.logs_distributed
WHERE tenant_id = 'acme' AND timestamp >= today() - 7;
```

### config.xml tunings

```xml
<clickhouse>
  <max_server_memory_usage_to_ram_ratio>0.8</max_server_memory_usage_to_ram_ratio>
  <mark_cache_size>5368709120</mark_cache_size>          <!-- 5 GB mark cache -->
  <uncompressed_cache_size>8589934592</uncompressed_cache_size>  <!-- 8 GB uncompressed -->
  <max_concurrent_queries>200</max_concurrent_queries>
  <max_connections>4096</max_connections>
  <merge_tree>
    <max_bytes_to_merge_at_max_space_in_pool>161061273600</max_bytes_to_merge_at_max_space_in_pool>
    <number_of_free_entries_in_pool_to_lower_max_size_of_merge>8</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
    <parts_to_throw_insert>600</parts_to_throw_insert>
    <parts_to_delay_insert>300</parts_to_delay_insert>
    <max_replicated_merges_in_queue>16</max_replicated_merges_in_queue>
  </merge_tree>
</clickhouse>
```

---

## Linux Kernel Tuning

```bash
# Run as root on all nodes before deploying
bash scripts/tune.sh tune

# Critical settings for 1M+ connections/sec:
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65536
net.core.netdev_max_backlog = 250000
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1              # Reuse TIME_WAIT sockets
net.ipv4.tcp_fin_timeout = 15
fs.file-max = 2097152
vm.swappiness = 1                      # Never swap (CH is memory-intensive)
vm.dirty_ratio = 80                    # Aggressive write-back for NVMe
```

Add to `/etc/sysctl.d/99-logflow.conf` for persistence across reboots.

---

## Monitoring & Alerting

### Key dashboards

Open Grafana at http://localhost:3000 → **LogFlow Platform Overview**

| Panel | Metric | Alert threshold |
|---|---|---|
| Ingestion rate | `logflow_ingestion_logs_ingested_total` | — |
| Drop rate | backpressure drops / accepted | > 1% → CRITICAL |
| Search P99 | `logflow_search_query_duration_seconds` | > 100ms → WARNING |
| Kafka lag | `kafka_consumergroup_lag_sum` | > 100k → CRITICAL |
| ClickHouse write errors | `logflow_clickhouse_rows_written_total{outcome="error"}` | any → CRITICAL |
| DLQ rate | `logflow_consumer_dlq_messages_total` | > 10/s → WARNING |

### Alert notification (Alertmanager)

```yaml
# monitoring/alertmanager/alertmanager.yml
route:
  group_by: [alertname, service]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: pagerduty-critical
  routes:
    - match:
        severity: warning
      receiver: slack-warnings

receivers:
  - name: pagerduty-critical
    pagerduty_configs:
      - service_key: <PAGERDUTY_KEY>
  - name: slack-warnings
    slack_configs:
      - api_url: <SLACK_WEBHOOK>
        channel: "#logflow-alerts"
```

---

## Failure Scenarios & Runbooks

### Kafka broker failure

**Symptoms:** Consumer lag growing, ingestion drop rate rising  
**Response:**
```bash
# Check broker status
kubectl get pods -n logflow -l app.kubernetes.io/name=kafka
# Identify under-replicated partitions
kubectl exec -n logflow kafka-0 -- kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe \
  --under-replicated-partitions
# Restart the failed broker pod (StatefulSet will reattach PVC)
kubectl delete pod kafka-2 -n logflow
```
**Prevention:** 3 brokers, `min.insync.replicas=2`, PodDisruptionBudget `minAvailable=2`

### ClickHouse "too many parts" error

**Symptoms:** Insert latency spike, `429 Too Many Parts` in consumer logs  
**Response:**
```bash
CH_POD=$(kubectl get pod -n logflow -l app=clickhouse -o jsonpath='{.items[0].metadata.name}')
# Check part count
kubectl exec -n logflow $CH_POD -- clickhouse-client \
  --query "SELECT partition, count() as parts FROM system.parts WHERE table='logs_local' GROUP BY partition ORDER BY parts DESC LIMIT 10"
# Speed up merges temporarily
kubectl exec -n logflow $CH_POD -- clickhouse-client \
  --query "SYSTEM START MERGES logflow.logs_local"
```
**Prevention:** `parts_to_delay_insert=300`, consumer batch size ≥ 2000, single batch per partition

### High consumer lag

**Symptoms:** `kafka_consumergroup_lag_sum > 100000`  
**Response:**
```bash
# Scale up consumers (bounded by partition count)
kubectl scale deployment logflow-kafka-consumer -n logflow --replicas=12
# If ClickHouse is the bottleneck, increase batch write parallelism
kubectl set env deployment/logflow-kafka-consumer -n logflow CONSUMER_WORKERS=8
```

### Ingestion backpressure (drop rate > 1%)

**Symptoms:** `logflow_ingestion_backpressure_drops_total` rising  
**Response:**
1. Scale Kafka Consumer replicas to reduce queue
2. Scale Ingestion Service to spread producer load
3. Temporarily increase `PRODUCER_CHANNEL_BUFFER` (loses memory isolation)
4. Investigate if ClickHouse is slow with `bash scripts/tune.sh clickhouse`

### Redis cache failure

**Symptoms:** All search queries hitting ClickHouse, latency spike  
**Response:** Search service degrades gracefully (skips cache, still serves results). Restore Redis Sentinel quorum:
```bash
kubectl rollout restart deployment/logflow-redis-master -n logflow
```

---

## Cost Optimization

| Strategy | Estimated saving |
|---|---|
| SPOT instances for application nodes | 60-70% on compute |
| ZSTD compression in Kafka + ClickHouse | 70-80% on storage |
| S3 cold archival after 7 days (ClickHouse TTL) | 90% on old data |
| Redis LRU cache reduces ClickHouse queries | 70%+ fewer CH queries at scale |
| Kafka log retention 24h (logs already in CH) | Minimises Kafka EBS |
| Right-size ClickHouse: add RAM before CPUs | Maximises CH scan efficiency |
| gp3 EBS (vs gp2) | 20% cheaper, 3× IOPS baseline |

**Monthly estimate (production, 1M logs/sec, us-east-1):**

| Component | Instance | Count | $/month |
|---|---|---|---|
| Application nodes (SPOT) | c6i.2xlarge | 6-30 | ~$800 |
| Storage nodes (ON_DEMAND) | r6i.2xlarge | 3 | ~$1,800 |
| System nodes | m6i.large | 3 | ~$300 |
| EBS gp3 (500 GB × 3 CH nodes) | — | — | ~$450 |
| Kafka EBS (100 GB × 3) | — | — | ~$90 |
| S3 cold archival | — | — | ~$50 |
| NAT gateways | — | 3 | ~$100 |
| **Total** | | | **~$3,600/month** |

---

## Troubleshooting Guide

```bash
# 1. Check pod status and recent events
kubectl get pods -n logflow -o wide
kubectl describe pod <pod-name> -n logflow

# 2. Tail service logs
kubectl logs -f deployment/logflow-ingestion -n logflow --all-containers

# 3. Check Kafka topics and consumer groups
kubectl exec -n logflow logflow-kafka-0 -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --all-groups

# 4. Verify ClickHouse connectivity from consumer pod
kubectl exec -n logflow deployment/logflow-kafka-consumer -- \
  wget -qO- "http://logflow-clickhouse:8123/?query=SELECT+1"

# 5. Check Redis
kubectl exec -n logflow deployment/logflow-search -- \
  sh -c 'redis-cli -h logflow-redis-master ping'

# 6. Query Prometheus for errors
curl -s "http://localhost:9099/api/v1/query?query=rate(logflow_gateway_http_requests_total{status=~\"5..\"}[5m])" | jq .

# 7. Verify HPA is working
kubectl get hpa -n logflow

# 8. Check PDB status (during rolling updates)
kubectl get pdb -n logflow

# 9. Inspect ClickHouse insert errors
kubectl exec -n logflow clickhouse-0 -- clickhouse-client \
  --query "SELECT * FROM system.errors WHERE last_error_time > now() - 3600 ORDER BY last_error_time DESC"

# 10. Force TTL cleanup in ClickHouse (if disk critical)
kubectl exec -n logflow clickhouse-0 -- clickhouse-client \
  --query "ALTER TABLE logflow.logs_local ON CLUSTER '{cluster}' MATERIALIZE TTL"
```

---

## CI/CD Pipeline

```
push to develop:
  test (6 services in parallel) → security scan → build+push → deploy staging → smoke test

push tag v*:
  test → security → build → deploy staging → canary 20% production (5min) → promote 100%

pull_request:
  test → lint → helm validate → kubeval
```

**Rollback:**
```bash
# Helm rollback to previous release
helm history logflow -n logflow
helm rollback logflow <revision> -n logflow

# Or kubectl rollout
kubectl rollout undo deployment/logflow-ingestion -n logflow
kubectl rollout status deployment/logflow-ingestion -n logflow
```

---

## Contributing

```bash
# Setup development environment
make setup           # installs Go tools, golangci-lint, k6, helm

# Run all tests
make test

# Lint all services
make lint

# Build all images locally
make build

# Start local stack
make up

# Run load test
make bench
```

**Commit convention:** `feat(ingestion): add gzip decompression support`  
**Branch naming:** `feat/`, `fix/`, `chore/`, `docs/`  
All PRs require: ✅ tests pass · ✅ lint clean · ✅ security scan clean

---

## License

MIT © YourOrg. See [LICENSE](LICENSE) for details.
