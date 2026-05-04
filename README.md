# PTBot Internal

A load-test orchestration platform that uses Temporal workflows to run k6 tests, ingest metrics into Prometheus, store metadata and logs in PostgreSQL/TimescaleDB, and expose Grafana dashboards.

## What this project does

This repository implements a service-oriented load testing system with two main components:

- **Backend**: orchestrates test runs using Temporal workflows and records results in PostgreSQL.
- **Runner**: executes `k6` load tests and streams runtime output back to the backend.

Monitoring is provided by **Prometheus** and **Grafana**.

## Architecture

The system follows a clean separation of concerns:

- `backend` contains the orchestration service.
- `runner` contains the executable service for launching k6 tests.
- `docker-compose.yml` starts backend, runner, Temporal, PostgreSQL, Prometheus, and Grafana.

## Folder structure

```
ptbot_internal/
├── backend/
│   ├── cmd/backend/        # Backend service entrypoint
│   │   ├── main.go
│   │   └── handler.go
│   ├── internal/
│   │   ├── app/            # Workflows and Temporal activities
│   │   │   ├── workflow.go
│   │   │   └── activities.go
│   │   ├── db/             # Database connection and persistence
│   │   │   └── db.go
│   │   ├── model/          # Shared request and metric types
│   │   │   └── models.go
│   │   ├── util/           # Small helpers
│   │   │   └── sanitize.go
│   │   └── worker/         # Temporal worker and client logic
│   │       └── worker.go
│   ├── Dockerfile
│   ├── go.mod
│   ├── go.sum
│   └── results/            # Runtime log files (ignored by git)
│       └── .gitkeep
├── runner/
│   ├── cmd/runner/         # Runner service entrypoint
│   │   └── main.go
│   ├── Dockerfile
│   └── go.mod
├── monitoring/             # Prometheus and Grafana config
├── docker-compose.yml
├── go.work                 # Workspace for backend + runner modules
├── .gitignore
└── README.md
```

## Core services

### backend

The backend is responsible for:

- accepting load-test requests
- starting Temporal workflows
- creating run metadata and log files
- sending the request to the runner service
- processing streamed runner output
- extracting metrics from logs
- storing metrics and logs in PostgreSQL
- updating run status

### runner

The runner does:

- accepts `POST /run-test`
- receives `run_id`, `vus`, and `script`
- launches `k6 run -` with the supplied script
- streams `stdout` / `stderr` back via server-sent events
- forwards Prometheus metrics to the Prometheus service

### monitoring

- Prometheus collects metrics from the runner
- Grafana visualizes metrics using preconfigured dashboards

## Docker setup

This project uses `docker-compose.yml` to start all services together:

- `backend`
- `runner`
- `temporal`
- `postgres`
- `prometheus`
- `grafana`

### Run the stack

```bash
cd /Users/darshanjain/Darshan/ptbot_internal
docker compose up --build
```

### Stop the stack

```bash
docker compose down
```

## Build locally

The repo uses a Go workspace for the two Go modules.

### Backend

```bash
cd backend
go build ./cmd/backend
```

### Runner

```bash
cd runner
go build ./cmd/runner
```

## Backend API

- `POST /run-test`
  - Request body:
    - `run_id` (optional)
    - `vus` (required)
    - `script` (required)
  - Behavior:
    - starts a Temporal workflow
    - streams workflow status updates as SSE

- `GET /health` (healthcheck)

## Important environment variables

### backend

- `BACKEND_PORT` - port for backend API (default `8081`)
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `RUNNER_URL` - runner service URL
- `PROMETHEUS_URL` - Prometheus base URL
- `TEMPORAL_SERVICE_ADDRESS` - Temporal host:port (default `temporal:7233`)

### runner

No special environment variables are required beyond Docker compose wiring.

## Notes

- The backend uses Temporal to make workflow execution reliable and observable.
- The runner executes k6 inside the container and streams logs in real time.
- `backend/results/` is used for temporary log storage and is ignored in git.

## Recommended improvements

- add a dedicated schema migration tool
- add request validation and authentication
- add a lightweight frontend or CLI for submitting tests
- add Prometheus query-based metrics export to the backend

## Quick summary

This repo is a service-based load-test orchestration platform with a clean separation between:

- workflow orchestration (`backend`)
- test execution (`runner`)
- monitoring (`Prometheus` + `Grafana`)
- storage (`PostgreSQL`)

If you want, I can also add an example request payload and a simple `make` file for startup commands.
