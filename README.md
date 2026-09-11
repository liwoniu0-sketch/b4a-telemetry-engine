# NexCloud™ Distributed APM & Telemetry Engine

High-performance, zero-dependency cloud-native telemetry ingestion and real-time edge latency benchmarking service.

---

## 🌟 Key Features

1. **Zero External Dependencies**: Built strictly using Node.js native runtime APIs (`node:http`, `node:net`, `node:crypto`, `node:os`). Rapid container startup with minimal memory footprint (~15MB RAM).
2. **Dual-Mode Stream Ingestion**:
   - Supports **Chunked Streaming Telemetry (HTTP/1.1 & HTTP/2 Stream Ingestion)**.
   - Fully compliant **RFC-6455 WebSocket Upgrade Multiplexer**.
3. **Interactive Visual Dashboard & API Benchmarks**:
   - Built-in real-time interactive APM console at `/`.
   - Live system health and latency diagnostics endpoints (`/api/metrics`, `/api/ping`, `/api/speedtest`).
4. **Cloud-Native & Container Ready**:
   - Pre-configured for **Back4app Containers**, **Docker**, **Render**, and **Cloud Foundry**.

---

## 🚀 Quick Deployment (Back4app Containers)

1. Connect your GitHub repository to Back4app Containers.
2. Select repository `b4a-telemetry-engine`.
3. Set the build type to **Dockerfile** (or Node.js auto-detect).
4. Deploy the container. The service will automatically listen on `$PORT` (default `8080`).

---

## ⚙️ Environment Variables (Optional)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8080` | HTTP listening port assigned by container runtime |
| `METRICS_PATH` | `/v1/telemetry/stream` | Edge stream ingestion endpoint |
| `AUTH_KEY` | `f7bc8576-1150-4292-ad40-48128704a035` | Node identification & telemetry validation key |
| `NODE_ENV` | `production` | Node environment flag |

---

## 📊 Health Check

- `GET /healthz` -> `{"status":"ok","service":"nex-telemetry-engine"}`
- `GET /api/ping` -> `{"status":"pong","node":"nex-edge-01"}`
