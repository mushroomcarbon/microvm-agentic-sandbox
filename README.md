# microvm-agentic-sandbox

A Kubernetes-based sandbox execution service. Provision isolated microVM environments on demand, run commands inside them, manage files, and control their lifecycle, all through a simple REST API; really good for agentic workloads because of speed, isolation, and (enough) persistence as compared to Lambda, for example, which would be better for a more traditional chatbot workflow.

Each sandbox runs in its own [Kata Containers](https://katacontainers.io/) microVM (Cloud Hypervisor backend), giving strong isolation between tenants without the overhead of full VMs.

## Features

- **Sandbox lifecycle**: create, inspect, and kill isolated microVM sessions
- **Command execution**: run foreground or background commands with full stdout/stderr capture and SSE streaming
- **Postgres-backed state**: survives adapter restarts; reconciles stale pods automatically on startup
- **Configurable sessions**: per-session lifetime deadlines, idle timeouts, and auto-pause/kill policies
- **Egress control**: `none`, `limited` (allowlist), or `full` network modes per sandbox

## Architecture

```
  Client (HTTP)
       |
       v
  +-----------------------+
  |   adapter             |  Go REST API + Postgres
  +-----------+-----------+
              |
        K8s API + gRPC
              |
              v
  +----------------------------+
  |  Kubernetes cluster        |
  |  Kata Containers (CLH VMM) |
  |                            |
  |  [ Pod: guest-agent ]      |  one per sandbox
  +----------------------------+
```

See [sandbox_oss_design.md](sandbox_oss_design.md) for a full architecture and API walkthrough.

## Components

| Directory | Description |
|-----------|-------------|
| `adapter/` | REST API server — sandbox lifecycle, Postgres state, K8s and gRPC integration |
| `guest-agent/` | gRPC server that runs inside each sandbox pod (exec, file I/O, process management) |
| `agent-client/` | CLI for talking to the guest agent directly (dev/testing tool) |

## Prerequisites

- **Go 1.22+**
- **Docker** — to build the guest-agent image
- **Kubernetes** with Kata Containers + KVM on the node pool (k3s works for local dev)
- **PostgreSQL 15+**

## Quick start

### 1. Start Postgres

```bash
docker run -d \
  -e POSTGRES_USER=sandbox_oss \
  -e POSTGRES_PASSWORD=sandbox_oss \
  -e POSTGRES_DB=sandbox_oss \
  -p 5432:5432 \
  postgres:15
```

### 2. Build the guest-agent image

```bash
cd guest-agent
docker build -t sandbox-oss/guest-agent:latest .

# For k3s — import so it doesn't need a registry
sudo k3s ctr images import <(docker save sandbox-oss/guest-agent:latest)
```

### 3. Run the adapter

```bash
cd adapter
go run . -addr :8080 -namespace default
```

The adapter picks up your cluster via `$KUBECONFIG` (falls back to `/etc/rancher/k3s/k3s.yaml`), applies the Postgres schema, reconciles any leftover rows from a previous run, and starts listening.

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
SESSION_ID="sb-..."   # from the create response

curl -s -X POST http://localhost:8080/api/v1/sandboxes/$SESSION_ID/exec \
  -H 'Content-Type: application/json' \
  -d '{"command": "echo hello from the sandbox"}' | jq .
```

### 6. Kill the sandbox

```bash
curl -s -X DELETE http://localhost:8080/api/v1/sandboxes/$SESSION_ID
```

## API reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/sandboxes` | Create a sandbox |
| `GET` | `/api/v1/sandboxes/{id}` | Get sandbox status |
| `DELETE` | `/api/v1/sandboxes/{id}` | Kill a sandbox |
| `POST` | `/api/v1/sandboxes/{id}/exec` | Run a command |
| `GET` | `/api/v1/sandboxes/{id}/exec` | List execs |
| `GET` | `/api/v1/sandboxes/{id}/exec/{exec_id}` | Get exec status |
| `GET` | `/api/v1/sandboxes/{id}/exec/{exec_id}/output` | Get captured output |
| `GET` | `/api/v1/sandboxes/{id}/exec/{exec_id}/output/stream` | Stream output (SSE) |
| `POST` | `/api/v1/sandboxes/{id}/exec/{exec_id}/stdin` | Write to stdin |
| `POST` | `/api/v1/sandboxes/{id}/exec/{exec_id}/signal` | Send a signal |
| `GET` | `/healthz` | Liveness probe |

See [sandbox_oss_design.md](sandbox_oss_design.md) for full request/response shapes.

## Configuration

The adapter accepts these flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:8080` | HTTP listen address |
| `-namespace` | `default` | Kubernetes namespace for sandbox pods |
| `-db-url` | `postgres://sandbox_oss:sandbox_oss@localhost:5432/sandbox_oss?sslmode=disable` | Postgres connection URL (also reads `DATABASE_URL` env) |

## License

MIT: see [LICENSE](LICENSE).
