# Sandbox OSS — Architecture & Usage Guide

Sandbox OSS is a Kubernetes-based sandbox execution service. It provisions isolated compute environments (sandboxes) on demand, runs commands inside them, and manages their full lifecycle. Each sandbox runs in its own Kata Containers microVM.

---

## Architecture

```
  Client (HTTP)
       |
       v
  +--------------------+
  |   sandbox-oss      |  ← Go REST adapter (this repo)
  |   adapter          |
  +--------+-----------+
           |
     K8s API + gRPC
           |
           v
  +----------------------------+
  |  Kubernetes cluster        |
  |  - Kata Containers runtime |
  |  - Cloud Hypervisor VMM   |
  |  - sandbox namespace      |
  |                            |
  |  [ Pod: guest-agent ]      |  ← one per sandbox
  +----------------------------+
```

**Components:**

| Component | Description |
|-----------|-------------|
| `adapter` | REST API server. Manages sandbox lifecycle, persists state to Postgres, and proxies exec/file operations to the guest agent over gRPC. |
| `guest-agent` | Go gRPC server that runs as the entrypoint of every sandbox pod. Handles command execution, file I/O, and process management inside the microVM. |
| `agent-client` | CLI for talking to the guest agent directly; not needed at runtime. |

---

## Sandbox Lifecycle

A sandbox moves through these states:

```
creating → running → [paused] → ended
```

- **creating**: pod is being scheduled and the guest agent is booting.
- **running**: guest agent is healthy; exec and file operations are accepted.
- **paused**: microVM snapshot is held on the node; no CPU/RAM consumed.
- **ended**: pod deleted, storage reclaimed; no further transitions.

Sessions expire at a configurable deadline. An idle timeout can auto-pause or auto-kill a sandbox that hasn't received activity within a configured window.

---

## API

Base URL: `GET|POST|DELETE /api/v1`

### Sandbox lifecycle

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sandboxes` | Create a sandbox |
| `GET` | `/sandboxes/{id}` | Get sandbox status |
| `DELETE` | `/sandboxes/{id}` | Kill a sandbox |

### Command execution

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sandboxes/{id}/exec` | Run a command (foreground or background) |
| `GET` | `/sandboxes/{id}/exec` | List execs for this sandbox |
| `GET` | `/sandboxes/{id}/exec/{exec_id}` | Get exec status |
| `GET` | `/sandboxes/{id}/exec/{exec_id}/output` | Get captured stdout/stderr |
| `GET` | `/sandboxes/{id}/exec/{exec_id}/output/stream` | Stream output (SSE) |
| `POST` | `/sandboxes/{id}/exec/{exec_id}/stdin` | Write to stdin |
| `POST` | `/sandboxes/{id}/exec/{exec_id}/signal` | Send a signal (SIGTERM, SIGKILL, …) |

### Create sandbox: request body

```json
{
  "flavor": "sandbox-small",
  "image_id": "sandbox-oss/guest-agent:latest",
  "tenant_id": "tenant-abc",
  "tags": { "purpose": "eval" },

  "max_session_seconds": 1800,
  "idle_timeout_seconds": 300,
  "idle_action": "pause",

  "environment": { "MY_VAR": "value" },
  "network_egress": "none",

  "callback_url": "https://your-server.example.com/sandbox-done"
}
```

`idle_action` is `"pause"` (default) or `"kill"`.
`network_egress` is `"none"` (default), `"limited"`, or `"full"`.

### Run a command: request body

```json
{
  "command": "python /work/eval.py",
  "cwd": "/work",
  "environment": { "MODE": "fast" },
  "background": false,
  "timeout_seconds": 60
}
```

Set `"background": true` to get an exec ID immediately and poll/stream output separately.

---

## Flavors

Flavors map to Kubernetes resource requests:

| Flavor | vCPU | RAM |
|--------|------|-----|
| `sandbox-micro` | 0.25 | 256 MB |
| `sandbox-small` | 1 | 1 GB |
| `sandbox-medium` | 2 | 4 GB |
| `sandbox-large` | 4 | 16 GB |

---

## Running Locally

### Prerequisites

- Go 1.22+
- Docker (for building the guest-agent image)
- [k3s](https://k3s.io/) or any Kubernetes cluster with Kata Containers + KVM available
- PostgreSQL 15+

### 1. Start Postgres

```bash
docker run -d \
  -e POSTGRES_USER=sandbox_oss \
  -e POSTGRES_PASSWORD=sandbox_oss \
  -e POSTGRES_DB=sandbox_oss \
  -p 5432:5432 \
  postgres:15
```

### 2. Build and load the guest-agent image

```bash
cd guest-agent
docker build -t sandbox-oss/guest-agent:latest .
# For k3s: import the image so it doesn't need a registry
sudo k3s ctr images import <(docker save sandbox-oss/guest-agent:latest)
```

### 3. Run the adapter

```bash
cd adapter
go run . \
  -addr :8080 \
  -namespace default \
  -db-url "postgres://sandbox_oss:sandbox_oss@localhost:5432/sandbox_oss?sslmode=disable"
```

The adapter connects to your cluster via `$KUBECONFIG` (falls back to `/etc/rancher/k3s/k3s.yaml`), applies the schema, reconciles any stale sandbox rows, and starts listening.

### 4. Create a sandbox

```bash
curl -s -X POST http://localhost:8080/api/v1/sandboxes \
  -H 'Content-Type: application/json' \
  -d '{
    "flavor": "sandbox-small",
    "image_id": "sandbox-oss/guest-agent:latest",
    "tenant_id": "local-test"
  }' | jq .
```

### 5. Run a command

```bash
SESSION_ID="<session_id from above>"

curl -s -X POST http://localhost:8080/api/v1/sandboxes/$SESSION_ID/exec \
  -H 'Content-Type: application/json' \
  -d '{"command": "echo hello from the sandbox"}' | jq .
```

### 6. Kill the sandbox

```bash
curl -s -X DELETE http://localhost:8080/api/v1/sandboxes/$SESSION_ID
```

---

## Project Structure

```
adapter/          REST API adapter (main entrypoint)
  internal/
    api/          HTTP handlers, middleware, request/response types
    sandbox/      Sandbox manager — lifecycle and Postgres state
    exec/         Exec registry — command tracking and output storage
    k8s/          Kubernetes client (pod create/delete/get)
    agent/        gRPC client for the in-pod guest agent
    db/           Postgres connection and schema migration

guest-agent/      gRPC server that runs inside each sandbox pod
  proto/          Protobuf definitions

agent-client/     CLI client for the guest agent (dev/test tool)
```

---

## Notes

- Kata Containers with Cloud Hypervisor (`kata-clh` RuntimeClass) is required on the cluster nodes. Each sandbox runs in its own microVM; the Kata runtime gives each pod a separate kernel.
- The adapter is stateless beyond Postgres, so restarting it reconciles stale rows and redials live pods automatically.
- The compiled `agent-client` binary is not needed at runtime; build it from source with `go build ./agent-client`.
