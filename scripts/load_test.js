/**
 * LogFlow Load Test — k6
 * Targets: 1M+ logs/sec ingestion, <100ms search P99
 *
 * Run: k6 run --env BASE_URL=http://api.logflow.yourorg.com \
 *             --env JWT_TOKEN=<token> \
 *             scripts/load_test.js
 *
 * Staged ramp:
 *   0→500 VUs over 2 min  → sustained 500 VUs for 5 min
 *   500→2000 VUs over 3 min → sustained 2000 VUs for 10 min
 *   2000→0 VUs over 2 min  (cool-down)
 */

import http from "k6/http";
import ws from "k6/ws";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";
import { randomItem, randomIntBetween } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

// ── Custom metrics ──────────────────────────────────────────────────────────
const ingestErrorRate   = new Rate("logflow_ingest_errors");
const searchErrorRate   = new Rate("logflow_search_errors");
const ingestLatency     = new Trend("logflow_ingest_latency", true);
const searchLatency     = new Trend("logflow_search_latency", true);
const logsAccepted      = new Counter("logflow_logs_accepted");
const logsDropped       = new Counter("logflow_logs_dropped");

// ── Config ──────────────────────────────────────────────────────────────────
const BASE_URL  = __ENV.BASE_URL  || "http://localhost:8080";
const JWT_TOKEN = __ENV.JWT_TOKEN || "replace-me";
const TENANT_ID = __ENV.TENANT_ID || "tenant-load-test";
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || "1000");

const HEADERS = {
  "Content-Type": "application/json",
  "Authorization": `Bearer ${JWT_TOKEN}`,
  "X-Tenant-ID": TENANT_ID,
};

// ── Load shape ───────────────────────────────────────────────────────────────
export const options = {
  stages: [
    { duration: "2m",  target: 500  },  // Ramp up
    { duration: "5m",  target: 500  },  // Sustained low
    { duration: "3m",  target: 2000 },  // Scale up
    { duration: "10m", target: 2000 },  // Peak load
    { duration: "2m",  target: 0    },  // Cool-down
  ],
  thresholds: {
    // SLA targets
    "logflow_ingest_latency{p:95}": ["p(95)<200"],    // 95th pct < 200ms
    "logflow_search_latency{p:99}": ["p(99)<100"],    // 99th pct < 100ms (SLA)
    "logflow_ingest_errors":        ["rate<0.001"],   // < 0.1% error rate
    "logflow_search_errors":        ["rate<0.001"],
    "http_req_duration{p:99}":      ["p(99)<1000"],
  },
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)", "p(99.9)"],
};

// ── Data generators ──────────────────────────────────────────────────────────
const SERVICES    = ["api-server", "worker", "scheduler", "auth", "gateway", "consumer"];
const NAMESPACES  = ["production", "staging", "default", "monitoring"];
const LEVELS      = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"];  // weighted
const ENVIRONMENTS = ["prod", "staging"];

function generateLog() {
  return {
    id:          uuidv4(),
    timestamp:   new Date().toISOString(),
    level:       randomItem(LEVELS),
    service:     randomItem(SERVICES),
    namespace:   randomItem(NAMESPACES),
    pod_name:    `${randomItem(SERVICES)}-${randomIntBetween(1000, 9999)}`,
    node_name:   `ip-10-0-${randomIntBetween(1,254)}-${randomIntBetween(1,254)}`,
    trace_id:    uuidv4().replace(/-/g, ""),
    span_id:     uuidv4().replace(/-/g, "").slice(0, 16),
    message:     `[${randomItem(LEVELS)}] Request processed in ${randomIntBetween(1, 500)}ms path=/api/v1/${randomItem(["logs","search","stream"])} status=${randomItem([200,200,200,400,500])}`,
    environment: randomItem(ENVIRONMENTS),
    host_ip:     `10.0.${randomIntBetween(0,254)}.${randomIntBetween(1,254)}`,
    labels: {
      version: `v1.${randomIntBetween(0,9)}.${randomIntBetween(0,9)}`,
      region:  "us-east-1",
    },
    attributes: {
      "http.method":      randomItem(["GET", "POST", "DELETE"]),
      "http.status_code": String(randomItem([200, 200, 400, 500])),
    },
  };
}

function generateBatch(size) {
  const logs = [];
  for (let i = 0; i < size; i++) {
    logs.push(generateLog());
  }
  return { logs };
}

// ── Scenarios ────────────────────────────────────────────────────────────────

// 80% of VUs hammer the ingestion endpoint.
export function ingest() {
  const payload = JSON.stringify(generateBatch(BATCH_SIZE));
  const start   = Date.now();
  const res     = http.post(`${BASE_URL}/api/v1/logs/batch`, payload, { headers: HEADERS });
  const took    = Date.now() - start;

  ingestLatency.add(took);
  const ok = check(res, {
    "ingest: status 202":   (r) => r.status === 202,
    "ingest: has accepted": (r) => r.json("accepted") > 0,
  });
  ingestErrorRate.add(!ok);

  if (ok && res.json("accepted")) {
    logsAccepted.add(res.json("accepted"));
    logsDropped.add(res.json("dropped") || 0);
  }
  sleep(0.1); // 100ms between batches per VU → ~10 batches/sec per VU
}

// 15% of VUs run search queries.
export function search() {
  const now  = new Date();
  const from = new Date(now - 30 * 60 * 1000); // last 30 minutes

  const params = new URLSearchParams({
    q:          randomItem(["error", "timeout", "failed", "200", "started"]),
    service:    randomItem(SERVICES),
    level:      randomItem(["INFO", "ERROR", "WARN"]),
    start_time: from.toISOString(),
    end_time:   now.toISOString(),
    page:       "1",
    page_size:  "50",
  });

  const start = Date.now();
  const res   = http.get(`${BASE_URL}/api/v1/logs/search?${params}`, { headers: HEADERS });
  const took  = Date.now() - start;

  searchLatency.add(took);
  const ok = check(res, {
    "search: status 200":  (r) => r.status === 200,
    "search: has logs key": (r) => Array.isArray(r.json("logs")),
  });
  searchErrorRate.add(!ok);
  sleep(0.5);
}

// 5% of VUs open a WebSocket for live tail.
export function liveTail() {
  const wsUrl = BASE_URL.replace("http://", "ws://").replace("https://", "wss://");
  const res   = ws.connect(
    `${wsUrl}/api/v1/stream/tail`,
    { headers: HEADERS },
    function (socket) {
      socket.on("open", () => {
        socket.send(JSON.stringify({
          service:   randomItem(SERVICES),
          namespace: randomItem(NAMESPACES),
          level:     randomItem(["INFO", "ERROR"]),
        }));
      });
      socket.on("message", (data) => {
        check(JSON.parse(data), { "ws: has message field": (m) => m.message !== undefined });
      });
      socket.on("error", (e) => { console.error("ws error", e); });
      socket.setTimeout(() => { socket.close(); }, 30000); // 30 second session
    }
  );
  check(res, { "ws: connected": (r) => r && r.status === 101 });
}

// ── Default function: mix all scenarios ─────────────────────────────────────
export default function () {
  const rand = Math.random();
  if (rand < 0.80) {
    ingest();
  } else if (rand < 0.95) {
    search();
  } else {
    liveTail();
  }
}

// ── Custom summary ───────────────────────────────────────────────────────────
export function handleSummary(data) {
  const ingestP99 = data.metrics["logflow_ingest_latency"]?.values?.["p(99)"] || 0;
  const searchP99 = data.metrics["logflow_search_latency"]?.values?.["p(99)"] || 0;
  const accepted  = data.metrics["logflow_logs_accepted"]?.values?.count || 0;
  const errRate   = (data.metrics["logflow_ingest_errors"]?.values?.rate || 0) * 100;

  console.log("═══════════════════════════════════════════════");
  console.log("  LogFlow Load Test Summary");
  console.log("═══════════════════════════════════════════════");
  console.log(`  Total logs accepted : ${accepted.toLocaleString()}`);
  console.log(`  Ingest P99 latency  : ${ingestP99.toFixed(2)}ms  (target <200ms)`);
  console.log(`  Search P99 latency  : ${searchP99.toFixed(2)}ms  (target <100ms)`);
  console.log(`  Ingest error rate   : ${errRate.toFixed(4)}%    (target <0.1%)`);
  console.log("═══════════════════════════════════════════════");

  return {
    "stdout": JSON.stringify(data, null, 2),
    "scripts/load_test_results.json": JSON.stringify(data),
  };
}
