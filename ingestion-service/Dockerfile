# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

ARG SERVICE=ingestion-service
ARG VERSION=dev
ARG COMMIT=unknown

RUN apk add --no-cache git ca-certificates tzdata upx

WORKDIR /workspace

# Layer-cache: copy module files first so go mod download only reruns on dependency changes.
COPY go.work go.work.sum ./
COPY ${SERVICE}/go.mod ${SERVICE}/go.sum ./${SERVICE}/

# Download dependencies before copying source (cache-friendly).
RUN go work sync && go mod download -modfile=${SERVICE}/go.mod

# Copy source.
COPY ${SERVICE}/ ./${SERVICE}/

# Build: static binary, strip debug symbols, compress.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w \
              -X main.Version=${VERSION} \
              -X main.Commit=${COMMIT} \
              -X main.BuildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o /bin/service \
    ./${SERVICE}/cmd/main.go

RUN upx --best --lzma /bin/service

# ── Stage 2: Distroless runtime ───────────────────────────────────────────────
# gcr.io/distroless/static-debian12 has no shell, no package manager, minimal attack surface.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /bin/service /service

# Run as non-root UID 65532 (distroless nonroot default).
USER nonroot:nonroot

EXPOSE 8080 9090

# Health check uses curl-free HTTP call — distroless has no shell.
# Kubernetes liveness/readiness probes should call /health directly.
ENTRYPOINT ["/service"]
