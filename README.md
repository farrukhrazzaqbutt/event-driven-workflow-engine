# Workflow Engine — Event-Driven Workflow Orchestration

A production-grade, event-driven workflow orchestration engine built with **FastAPI**, **Apache Kafka**, and **Redis**. The system accepts JSON-based workflow definitions (Directed Acyclic Graphs), validates them, and orchestrates task execution across distributed workers with full support for parallel branches, inter-node data passing, and race-condition-safe fan-in aggregation.

---

## Architecture Overview

```
┌──────────┐       ┌────────────────┐       ┌──────────────┐
│  Client   │──────▶│   FastAPI API   │──────▶│    Redis     │
│  (curl)   │◀──────│   (port 8000)  │◀──────│  (state DB)  │
└──────────┘       └───────┬────────┘       └──────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │  Kafka   │ │  Kafka   │ │  Kafka   │
        │  tasks   │ │  results │ │  topics  │
        └────┬─────┘ └────▲─────┘ └──────────┘
             │             │
             ▼             │
        ┌──────────────────┴──────┐
        │    Worker(s)            │
        │  (consume tasks,        │
        │   produce results)      │
        └─────────────────────────┘
```

**Components:**

| Component | Role |
|-----------|------|
| **FastAPI API** | Accepts workflow submissions, triggers execution, serves status/results. Runs the orchestrator's result consumer in the background. |
| **Redis** | Persists workflow execution state (node states, outputs). Uses optimistic locking (WATCH/MULTI/EXEC) for atomic updates. |
| **Kafka** | Message broker with two topics: `workflow.tasks` (API → Workers) and `workflow.results` (Workers → API/Orchestrator). |
| **Worker(s)** | Stateless consumers that execute task handlers and publish results. Horizontally scalable. |

---

## Quick Start

### Prerequisites

- Docker & Docker Compose v2+

### Launch All Services

```bash
docker compose up --build -d
```

This starts **Redis**, **Kafka** (KRaft mode, no Zookeeper), the **API** server, and **2 worker replicas**.

### Verify Health

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

---

## API Reference

### 1. Submit a Workflow

```bash
POST /workflow
Content-Type: application/json
```

**Request body** — the sample payload from the challenge:

```bash
curl -s -X POST http://localhost:8000/workflow \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Parallel API Fetcher",
    "dag": {
      "nodes": [
        {"id": "input", "handler": "input", "dependencies": []},
        {"id": "get_user", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://localhost:8911/document/policy/list"}},
        {"id": "get_posts", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://localhost:8911/document/policy/list"}},
        {"id": "get_comments", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://localhost:8911/document/policy/list"}},
        {"id": "output", "handler": "output", "dependencies": ["get_user", "get_posts", "get_comments"]}
      ]
    }
  }'
```

**Response (201):**
```json
{
  "execution_id": "a1b2c3d4-...",
  "message": "Workflow registered successfully."
}
```

### 2. Trigger Execution

```bash
POST /workflow/trigger/{execution_id}
```

```bash
curl -s -X POST http://localhost:8000/workflow/trigger/<execution_id> \
  -H 'Content-Type: application/json' \
  -d '{"node_params": {"user_id": 42}}'
```

**Response (200):**
```json
{
  "execution_id": "a1b2c3d4-...",
  "message": "Workflow execution triggered."
}
```

### 3. Check Status

```bash
GET /workflows/{execution_id}
```

```bash
curl -s http://localhost:8000/workflows/<execution_id> | python3 -m json.tool
```

**Response (200):**
```json
{
  "execution_id": "a1b2c3d4-...",
  "name": "Parallel API Fetcher",
  "state": "RUNNING",
  "nodes": {
    "input": "COMPLETED",
    "get_user": "RUNNING",
    "get_posts": "RUNNING",
    "get_comments": "COMPLETED",
    "output": "PENDING"
  }
}
```

### 4. Get Results

```bash
GET /workflows/{execution_id}/results
```

```bash
curl -s http://localhost:8000/workflows/<execution_id>/results | python3 -m json.tool
```

---

## Running Tests

Tests are designed to run **without** Kafka or Redis (all external dependencies are mocked).

**With Docker Compose:**

```bash
docker compose --profile test run --rm test
```

**Locally:**

```bash
pip install -r requirements.txt
pytest -v
```

---

## Project Structure

```
workflow-engine/
├── app/
│   ├── api/
│   │   └── routes.py            # FastAPI endpoint definitions
│   ├── core/
│   │   ├── config.py            # Environment-based configuration
│   │   ├── kafka_client.py      # Kafka producer/consumer helpers
│   │   └── redis_client.py      # Async Redis connection pool
│   ├── models/
│   │   └── schemas.py           # Pydantic models (request/response/internal)
│   ├── services/
│   │   ├── dag_validator.py     # Cycle detection & structural validation
│   │   ├── orchestrator.py      # Dispatch tasks, consume results, advance DAG
│   │   ├── state_manager.py     # Redis-backed workflow state persistence
│   │   └── template_resolver.py # {{ node.key }} template resolution
│   ├── workers/
│   │   └── task_worker.py       # Kafka consumer + mock handlers
│   └── main.py                  # FastAPI app with lifespan hooks
├── tests/
│   ├── test_api.py              # API endpoint integration tests
│   ├── test_dag_validator.py    # DAG validation unit tests
│   ├── test_template_resolver.py# Template resolution unit tests
│   └── test_workers.py          # Worker handler unit tests
├── worker_main.py               # Standalone worker entry-point
├── docker-compose.yml           # Full stack orchestration
├── Dockerfile                   # Multi-purpose container image
├── requirements.txt             # Python dependencies
├── DESIGN.md                    # Design decisions document
└── README.md                    # This file
```

---

## Triggering Test Workflows

### Scenario A — Linear (A → B → C)

```bash
curl -s -X POST http://localhost:8000/workflow \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Linear Flow",
    "dag": {
      "nodes": [
        {"id": "A", "handler": "input", "dependencies": []},
        {"id": "B", "handler": "call_external_service", "dependencies": ["A"], "config": {"url": "http://example.com/api"}},
        {"id": "C", "handler": "output", "dependencies": ["B"]}
      ]
    }
  }'
```

### Scenario B — Fan-Out / Fan-In

```bash
curl -s -X POST http://localhost:8000/workflow \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Fan-Out Fan-In",
    "dag": {
      "nodes": [
        {"id": "A", "handler": "input", "dependencies": []},
        {"id": "B", "handler": "call_external_service", "dependencies": ["A"], "config": {"url": "http://example.com/b"}},
        {"id": "C", "handler": "call_external_service", "dependencies": ["A"], "config": {"url": "http://example.com/c"}},
        {"id": "D", "handler": "output", "dependencies": ["B", "C"]}
      ]
    }
  }'
```

Then trigger and poll status as shown above.
