# ![Arke message broker proxy](doc/images/arke_logo.png)

## Overview

Arke is a message broker proxy with a gRPC front-end interface that supports
pluggable back-end message brokers. Currently supported: **AMQP 0.9.1** (e.g. RabbitMQ, including RabbitMQ Streams).

> In Greek mythology, Arke is the messenger for the Titans and is sometimes
> affiliated with the faded second rainbow seen in the shadow of the first.
> [Wikipedia: Arke](https://en.wikipedia.org/wiki/Arke)

Arke lets your application talk to a single gRPC interface while Arke handles
the broker-specific protocol on the back end. Swapping or upgrading a message
broker does not require changes to your application. This abstraction also gives
operators centralized control over which broker features are exposed and how they
are used, without requiring modifications to individual applications.

```text
Your Application
     │  gRPC
     ▼
  [ Arke ]
     │  AMQP 0.9.1 / Streams
     ▼
 Message Broker
 (e.g., RabbitMQ)
```

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running](#running)
- [gRPC API](#grpc-api)
- [TLS](#tls)
- [Rate Limiting](#rate-limiting)
- [Observability](#observability)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

---

## Features

- **gRPC front-end** – `Producer`, `Consumer`, and `Healthz` services.
- **Pluggable back-end providers** – Register new broker types without changing the API.
- **AMQP 0.9.1 support** – Full publish, consume, ack/nack, retry, and dead-letter workflows. RabbitMQ Streams are supported as a target type.
- **TLS** – Optional TLS on the gRPC listener with configurable certificate paths.
- **Rate limiting** – Per-client token-bucket rate limiter applied to `Connect`, `Publish`, and `Consume` RPCs.
- **Prometheus metrics** – HTTP metrics endpoint served on the same port as gRPC via connection multiplexing.
- **OpenTelemetry tracing** – Trace context propagated through all RPCs and forwarded to an OTLP gRPC collector.
- **Health service** – Standard gRPC health protocol plus a bidirectional `Healthz` stream that reports CPU/memory availability.
- **Message deduplication** – `publish_id` + `publisher_name` fields on `Message` enable deduplication on Streams.
- **Kubernetes awareness** – Monitors HPA replica counts and broadcasts `GOAWAY` health signals to clients when scaling up.
- **Source options** – Per-subscription options including `MessageTTL`, `Expires`, `DeadLetterAddress`, `DeadLetterSubject`, `Offset`, and `ConsumerGroup`.
- **Header filtering** – `Filter`/`Match` messages on source subscriptions for server-side message filtering.

---

## Requirements

- **Go 1.25+**
- A running AMQP 0.9.1-compatible broker (e.g., [RabbitMQ](https://www.rabbitmq.com/)) for back-end connectivity.
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` – only required if regenerating protobuf files.
- `golangci-lint` – only required for linting.
- Docker + Docker Compose – only required for integration tests.

---

## Installation

### Build from Source

```bash
# Clone the repository
git clone <repo-url>
cd arke

# Build for your current platform
make build

# Or build for a specific platform
make linux    # → build/linux/arke
make darwin   # → build/darwin/arke
make windows  # → build/windows/arke

# Build for all platforms
make all
```

---

## Configuration

Arke is configured entirely through environment variables.

### Core

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `50051` | TCP port Arke listens on for both gRPC and HTTP (Prometheus). |
| `CERT_FILE` | *(none)* | Path to the PEM-encoded TLS certificate file. Required together with `CERT_KEY` to enable TLS. |
| `CERT_KEY` | *(none)* | Path to the PEM-encoded TLS private key file. |

### Rate Limiting Variables

| Variable | Default | Description |
| --- | --- | --- |
| `RATE_LIMIT_ENFORCED` | `false` | Set to `true` to enforce the rate limit (requests over the limit are rejected). When `false`, violations are logged but allowed. |
| `RATE_LIMIT_BUCKET_SIZE` | *(disabled)* | Token-bucket capacity per client. Must be a positive integer. |
| `RATE_LIMIT_REFILL_SECONDS` | *(disabled)* | Interval in seconds at which the bucket refills one token. |
| `RATE_LIMIT_MAX_AGE_STALE_CLIENTS` | *(disabled)* | Seconds of inactivity before a client's rate-limit state is evicted. |

Rate limiting is disabled unless all three numeric variables are set to valid positive values.

### TLS (Provider Side)

| Variable | Description |
| --- | --- |
| `SAS_TRUSTED_CA_CERTIFICATES_PEM_FILE` | Path to a PEM file containing CA certificates trusted for broker connections. |

### Observability Variables

| Variable | Default | Description |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | gRPC endpoint of the OpenTelemetry collector. |
| `OTEL_SDK_DISABLED` | `false` | Set to `true` to disable OpenTelemetry tracing entirely. |

---

## Running

```bash
# Minimal – plain TCP, no TLS, no rate limiting
PORT=50051 ./build/darwin/arke

# With TLS
CERT_FILE=/path/to/cert.pem CERT_KEY=/path/to/key.pem ./build/darwin/arke

# CPU and heap profiling
./build/darwin/arke --cpuprofile cpu.out --memprofile mem.out
```

The process handles `SIGINT` (Ctrl-C) for a graceful shutdown.

---

## gRPC API

The protobuf specification lives in [api/protobuf-spec/arke.proto](api/protobuf-spec/arke.proto).
Generated protocol documentation is in [doc/arke_protocol.md](doc/arke_protocol.md).

Arke exposes three gRPC services(Producer, Consumer, and Healthz). Please see protocol documentation for details.

---

## TLS

### Server (Arke listener)

Set `CERT_FILE` and `CERT_KEY` to enable TLS on the gRPC listener. The listener
uses `h2` and `http/1.1` ALPN protocols so that both gRPC and the Prometheus HTTP
endpoint remain accessible on the same port.

### Back-end (Broker connection)

Pass `tls: true` (and optionally `ca_certificate` bytes) in `ConnectionConfiguration`
when calling `Connect`. Arke will use the provided CA certificate for verification,
or fall back to the system trust store if none is provided.
Set `SAS_TRUSTED_CA_CERTIFICATES_PEM_FILE` to supply additional trusted CA certificates
globally at the process level.

Use `--tls-skip-verify` to disable certificate verification entirely (not recommended for production).

---

## Rate Limiting

Arke implements a per-client token-bucket rate limiter. The limiter is applied to
the `Connect`, `Publish`, and `Consume` RPCs.

- Each client starts with a full bucket of `RATE_LIMIT_BUCKET_SIZE` tokens.
- One token is consumed per RPC call.
- The bucket refills at the rate of one token per `RATE_LIMIT_REFILL_SECONDS`.
- Client state is evicted after `RATE_LIMIT_MAX_AGE_STALE_CLIENTS` seconds of inactivity.
- When `RATE_LIMIT_ENFORCED=true`, clients that exceed the limit receive a `RESOURCE_EXHAUSTED` gRPC status.
- When `RATE_LIMIT_ENFORCED=false` (default), violations are logged but requests are allowed.

---

## Observability

### Prometheus Metrics

Prometheus metrics are available over HTTP on the same port as gRPC (default `50051`).
Arke uses connection multiplexing (`cmux`) to serve both protocols on a single listener.
Scrape endpoint: `http://<host>:<port>/metrics`

### OpenTelemetry Tracing

Traces are exported via OTLP gRPC to the endpoint specified by `OTEL_EXPORTER_OTLP_ENDPOINT`
(default: `localhost:4317`). Each RPC propagates trace context using W3C `traceparent`/`tracestate`
and B3 (`X-B3-TraceId` / `X-B3-SpanId`) headers.

Disable tracing by setting `OTEL_SDK_DISABLED=true`.

### Health Check

The standard gRPC health protocol (`grpc.health.v1.Health`) is registered on the server
and can be used by Kubernetes liveness/readiness probes. The custom `Healthz` service
additionally reports CPU availability, memory availability, and can signal clients to
reconnect (`GOAWAY`) when the pod is being scaled up. Handling the `GOAWAY` is not mandatory, but should be respected in an environment where Arke is loadbalanced to distribute the load.

---

## Development

### Running Unit Tests

```bash
make test
```

This runs `golangci-lint` followed by all unit tests with coverage output.
An HTML coverage report is written to `coverage.html`.

### Running Integration Tests

Integration tests require Docker and Docker Compose and spin up a RabbitMQ container.

```bash
# Build Arke for Linux and start RabbitMQ + Arke via Docker Compose, then run tests
make integration

# TLS variant
make integration_test_tls

# All integration test variants
make integration_all
```

### Regenerating Protocol Buffers

```bash
make generate
```

Requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`. Generated files are
written to [api/](api/). Documentation is regenerated to [doc/arke_protocol.md](doc/arke_protocol.md)
if `protoc-gen-doc` is also installed.

### Linting

```bash
make lint
```

### Makefile Targets

Run `make help` for a complete list of available targets.

---

## Contributing

Maintainers are accepting patches and contributions to this project.
Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details about submitting contributions to this project.

---

## Security Policy

Please see our [Security Policy](SECURITY.md) for details.

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
