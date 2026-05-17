#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════
#  LogFlow — Performance Tuning & Benchmark Script
#  Usage:
#    ./scripts/tune.sh            # apply kernel tuning (requires root)
#    ./scripts/tune.sh bench      # run ingestion benchmark
#    ./scripts/tune.sh clickhouse # ClickHouse perf check
# ════════════════════════════════════════════════════════════════════
set -euo pipefail

MODE="${1:-tune}"
BASE_URL="${LOGFLOW_URL:-http://localhost:8080}"
JWT_TOKEN="${LOGFLOW_TOKEN:-replace-me}"

# ── Kernel tuning for high-throughput networking ────────────────────────────
tune_kernel() {
  echo "==> Applying Linux kernel tuning for 1M+ logs/sec..."

  # Network core
  sysctl -w net.core.somaxconn=65535
  sysctl -w net.core.netdev_max_backlog=250000
  sysctl -w net.core.rmem_max=134217728
  sysctl -w net.core.wmem_max=134217728
  sysctl -w net.core.rmem_default=33554432
  sysctl -w net.core.wmem_default=33554432

  # TCP tuning
  sysctl -w net.ipv4.tcp_rmem="4096 33554432 134217728"
  sysctl -w net.ipv4.tcp_wmem="4096 33554432 134217728"
  sysctl -w net.ipv4.tcp_mem="786432 1048576 26777216"
  sysctl -w net.ipv4.tcp_max_syn_backlog=65536
  sysctl -w net.ipv4.tcp_tw_reuse=1
  sysctl -w net.ipv4.tcp_fin_timeout=15
  sysctl -w net.ipv4.tcp_keepalive_time=300
  sysctl -w net.ipv4.tcp_keepalive_intvl=30
  sysctl -w net.ipv4.tcp_keepalive_probes=3
  sysctl -w net.ipv4.ip_local_port_range="1024 65535"
  sysctl -w net.ipv4.tcp_max_tw_buckets=2000000
  sysctl -w net.ipv4.tcp_slow_start_after_idle=0
  sysctl -w net.ipv4.tcp_fastopen=3

  # File descriptors
  sysctl -w fs.file-max=2097152
  sysctl -w fs.nr_open=2097152
  ulimit -n 1048576

  # VM tuning for ClickHouse
  sysctl -w vm.swappiness=1
  sysctl -w vm.dirty_ratio=80
  sysctl -w vm.dirty_background_ratio=5
  sysctl -w vm.overcommit_memory=1
  sysctl -w vm.max_map_count=262144

  # IRQ affinity (spread across cores)
  if command -v irqbalance &>/dev/null; then
    systemctl enable --now irqbalance
  fi

  # Transparent huge pages — disable for ClickHouse (causes latency spikes)
  echo never > /sys/kernel/mm/transparent_hugepage/enabled
  echo never > /sys/kernel/mm/transparent_hugepage/defrag

  echo "==> Kernel tuning applied."
  echo "==> Persist by adding to /etc/sysctl.d/99-logflow.conf"
}

# ── Ingestion benchmark (curl-based, no external deps) ─────────────────────
bench_ingest() {
  echo "==> Running ingestion benchmark against $BASE_URL"
  BATCH_SIZE="${BATCH_BENCHMARK_BATCH:-1000}"
  ITERATIONS="${BATCH_BENCHMARK_ITER:-100}"
  CONCURRENCY="${BATCH_BENCHMARK_CONC:-10}"
  TOTAL=$((BATCH_SIZE * ITERATIONS * CONCURRENCY))

  PAYLOAD=$(python3 -c "
import json, random, time, uuid
logs = []
levels = ['INFO','WARN','ERROR','DEBUG']
services = ['api','worker','auth','scheduler']
for i in range($BATCH_SIZE):
    logs.append({
        'id': str(uuid.uuid4()),
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'level': random.choice(levels),
        'service': random.choice(services),
        'namespace': 'production',
        'pod_name': f'svc-{random.randint(1000,9999)}',
        'message': f'Request processed path=/api/v1/logs status=200 duration={random.randint(1,500)}ms',
        'labels': {'version': 'v1.0.0'},
        'attributes': {'http.status_code': '200'}
    })
print(json.dumps({'logs': logs}))
")

  TMP=$(mktemp)
  echo "$PAYLOAD" > "$TMP"

  echo "  Batch size  : $BATCH_SIZE"
  echo "  Iterations  : $ITERATIONS"
  echo "  Concurrency : $CONCURRENCY"
  echo "  Total logs  : $TOTAL"
  echo ""

  START=$(date +%s%3N)

  run_batch() {
    for _ in $(seq 1 "$ITERATIONS"); do
      curl -s -o /dev/null -w "%{http_code} %{time_total}\n" \
        -X POST "$BASE_URL/api/v1/logs/batch" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $JWT_TOKEN" \
        -H "X-Tenant-ID: bench-tenant" \
        --data-binary @"$TMP" || true
    done
  }

  PIDS=()
  for _ in $(seq 1 "$CONCURRENCY"); do
    run_batch &
    PIDS+=($!)
  done
  for pid in "${PIDS[@]}"; do wait "$pid"; done

  END=$(date +%s%3N)
  ELAPSED=$(( (END - START) ))
  LOGS_PER_SEC=$(( TOTAL * 1000 / ELAPSED ))

  rm -f "$TMP"
  echo ""
  echo "══════════════════════════════════════════"
  echo "  Benchmark Results"
  echo "══════════════════════════════════════════"
  printf "  Elapsed       : %d ms\n" "$ELAPSED"
  printf "  Total logs    : %'d\n" "$TOTAL"
  printf "  Throughput    : %'d logs/sec\n" "$LOGS_PER_SEC"
  echo "══════════════════════════════════════════"
}

# ── ClickHouse perf check ───────────────────────────────────────────────────
check_clickhouse() {
  CH_HOST="${CLICKHOUSE_HOST:-localhost}"
  CH_PORT="${CLICKHOUSE_PORT:-8123}"
  echo "==> ClickHouse performance diagnostics at $CH_HOST:$CH_PORT"

  run_query() {
    curl -s "http://${CH_HOST}:${CH_PORT}/?query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$1'))")"
  }

  echo ""
  echo "-- Active mutations / parts merges:"
  run_query "SELECT database, table, num_parts, result_part_name FROM system.merges LIMIT 20"

  echo ""
  echo "-- Largest partitions:"
  run_query "SELECT partition, sum(rows), formatReadableSize(sum(bytes_on_disk)) as size FROM system.parts WHERE database='logflow' GROUP BY partition ORDER BY sum(rows) DESC LIMIT 10"

  echo ""
  echo "-- Recent slow queries (>1s):"
  run_query "SELECT query_start_time, query_duration_ms, query FROM system.query_log WHERE query_duration_ms > 1000 AND type = 'QueryFinish' ORDER BY query_start_time DESC LIMIT 10"

  echo ""
  echo "-- Current memory usage:"
  run_query "SELECT metric, value FROM system.metrics WHERE metric IN ('MemoryTracking','MergesMutations','ReplicatedChecks')"
}

# ── Main ────────────────────────────────────────────────────────────────────
case "$MODE" in
  tune)       tune_kernel ;;
  bench)      bench_ingest ;;
  clickhouse) check_clickhouse ;;
  all)        tune_kernel; bench_ingest; check_clickhouse ;;
  *)
    echo "Usage: $0 [tune|bench|clickhouse|all]"
    exit 1
    ;;
esac
