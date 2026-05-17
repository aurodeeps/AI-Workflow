# Multi-Tenant AI Workflow Orchestration Platform

This project is a Go backend for running multi-tenant AI workflows over uploaded documents.

At a high level, the platform lets a tenant:

1. upload documents
2. store and chunk document content for retrieval
3. define workflows as JSON graphs
4. execute those workflows in sync or async mode
5. retrieve relevant chunks for a query
6. run LLM-style, condition, approval, audit, or HTTP tool steps
7. inspect run history, retries, dead-letter outcomes, and audit events

The project is mainly focused on backend architecture rather than frontend UX: tenant isolation, workflow execution, async job processing, retries, dead-letter handling, persistence, observability, and benchmarking.

## What It Includes

- tenant-scoped API key authentication
- document upload and chunking
- local object storage or MinIO-backed object storage
- JSON workflow definitions and validation
- sync and async workflow execution
- approval pause/resume flow
- audit event tracking
- Redis-backed async queue support
- Postgres-backed persistence for tenants, API keys, documents, workflows, runs, steps, pending approvals, and audit events
- local benchmark runner

## Architecture Overview

The main runtime pieces are:

- **API server**: accepts authenticated requests for document upload, workflow creation, workflow execution, and run inspection
- **Document service**: stores uploaded files and chunks text content for retrieval
- **Workflow service**: validates and stores workflow graph definitions
- **Executor**: walks through workflow nodes and records run and step state
- **Worker**: processes async jobs, schedules retries, and moves exhausted jobs to dead-letter state
- **Persistence layer**: uses in-memory repositories by default, or Postgres-backed repositories when `POSTGRES_DSN` is configured
- **Queue layer**: uses an in-memory queue in local in-process mode, or Redis for standalone async processing

## Supported Workflow Node Types

The workflow validator/executor currently supports these node types:

- `retrieve_documents`
- `llm`
- `condition`
- `http_tool`
- `approval`
- `audit_log`

## Execution Flow

A typical async workflow run looks like this:

1. a tenant sends a request with `X-API-Key`
2. the API authenticates the key and resolves tenant identity
3. a workflow run is created and queued
4. the worker dequeues the job and executes workflow nodes in order
5. steps can retrieve chunks, call the mock LLM provider, branch, call HTTP tools, or pause for approval
6. failed async runs are retried with backoff
7. exhausted runs are marked as dead-lettered
8. run state, step state, pending approval state, and audit events are persisted

## Persistence Modes

This repo now supports two repository modes:

### In-memory mode

If `POSTGRES_DSN` is not set, the API uses in-memory repositories for app state. This is useful for lightweight local experiments and the benchmark path.

### Postgres-backed mode

If `POSTGRES_DSN` is set, the API and worker use Postgres-backed repositories for:

- tenants
- API keys
- documents
- document chunks
- workflows
- workflow runs
- workflow steps
- pending approval state
- audit events

## Infrastructure

Local development infrastructure is defined in [docker-compose.yml](docker-compose.yml):

- PostgreSQL
- Redis
- MinIO
- API container
- worker container

Environment defaults live in [.env.example](.env.example).

## Database Schema

SQL migrations live in the [migrations](migrations) directory and now cover:

- tenants and API keys
- documents and document chunks
- workflows
- workflow runs and workflow steps
- pending approval state
- audit events

## Run It Locally

Start infrastructure:

```bash
docker compose up -d postgres redis minio
```

Run the API:

```bash
make run-api
```

Run the worker:

```bash
make run-worker
```

Run tests:

```bash
make test
```

Run the benchmark:

```bash
make bench BENCH_ARGS="-tenants 4 -docs-per-tenant 200 -doc-words 900 -sync-runs 1200 -async-runs 800 -concurrency 24"
```

Default dev API key from [.env.example](.env.example):

```text
example-key-tenant-a
```

## Current Notes and Limitations

- the default LLM integration is still a **mock provider**, which is useful for testing execution flow but not for production inference
- document chunk retrieval is still **simple lexical scoring**, not embedding/vector retrieval yet
- PDF uploads are accepted, but text chunking is currently strongest for text-based inputs
- Postgres persistence is wired in code, but you still need to make sure migrations are applied in your local environment before running in database-backed mode
- the benchmark numbers in this repo come from the in-process benchmark path and are useful for relative comparison, not production sizing

## Results

Measured with the local benchmark runner in this repo:

- small run: `10` documents, `20` workflow runs, `124.13 runs/sec`, `95.00%` success rate
- normal run: `800` documents, `2000` workflow runs, `7219.51 runs/sec`, `99.20%` success rate
- large run: `3000` documents, `6500` workflow runs, `10045.73 runs/sec`, `98.77%` success rate

## Project Layout

```text
cmd/
  api/      API entrypoint
  worker/   standalone async worker entrypoint
  bench/    local benchmark entrypoint

internal/
  api/         HTTP handlers and auth middleware
  audit/       audit event service and repositories
  auth/        API key authentication
  documents/   document storage and chunking
  executor/    workflow execution engine
  tenant/      tenant models and execution controls
  worker/      in-memory and Redis queue implementations
  workflow/    workflow definition and validation
  platform/    config, persistence, logging, health, tracing, IDs

migrations/    SQL schema migrations
scripts/       helper scripts
tests/         unit and integration tests
```

## Tech Stack

- Go
- PostgreSQL
- Redis
- MinIO / local object storage
- Docker Compose
- OpenTelemetry

## Notes

- benchmark command: `go run ./cmd/bench ...`
- tests live under `tests/`
- the worker is now intended to run as a real standalone process when Redis is configured
