# Workflow Engine — Project Overview

This document explains what the project does, how it works, and how to call the APIs in order.

---

## What Is It?

The **Workflow Engine** is an **event-driven orchestration system**. You send it a **workflow** (a graph of steps with dependencies). The engine:

1. **Validates** the graph (no cycles, all dependencies exist).
2. **Stores** the workflow and gives you an `execution_id`.
3. When you **trigger** that execution, it runs the steps in the right order:
   - Steps with no dependencies run first.
   - Steps that depend on others run only after their dependencies finish.
   - Independent steps run in **parallel** (e.g. multiple “call API” steps at once).
4. You can **poll status** and **get final results** when the workflow is done.

All of this is **asynchronous**: the API responds immediately; actual work is done by **workers** that consume tasks from **Kafka** and write state to **Redis**.

---

## High-Level Architecture

```
┌─────────────┐     HTTP      ┌─────────────────┐     Redis      ┌───────┐
│   Client    │ ◀──────────▶ │  FastAPI (API)   │ ◀────────────▶ │ Redis │
│ (Postman)   │               │  port 8000       │                │ :6379  │
└─────────────┘               └────────┬────────┘                └───────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
                    ▼                   ▼                   ▼
             ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
             │ Kafka       │    │ Kafka        │    │ Orchestrator│
             │ workflow.   │    │ workflow.    │    │ (inside API)│
             │ tasks      │    │ results      │    │ consumes    │
             └──────┬──────┘    └──────▲───────┘    │ results     │
                    │                  │            └──────┬──────┘
                    │                  │                   │
                    ▼                  │                   │ dispatch
             ┌─────────────┐           │                   │ ready nodes
             │  Worker(s)  │───────────┘                   │
             │  run steps  │  publish results               │
             └─────────────┘                                │
                    ▲                                      │
                    └──────────────────────────────────────┘
```

- **API**: Receives workflow JSON, validates it, stores execution in Redis, triggers runs, and serves status/results.
- **Orchestrator**: Runs inside the API process. Listens to the **workflow.results** Kafka topic; when a worker finishes a step, it updates Redis and dispatches the next **ready** steps to **workflow.tasks**.
- **Workers**: Separate processes. Consume from **workflow.tasks**, run the step (mock HTTP, mock LLM, or aggregate output), then publish to **workflow.results**.
- **Redis**: Single source of truth for each execution (which nodes are PENDING/RUNNING/COMPLETED/FAILED and their outputs).

---

## Main Concepts

### 1. Workflow = DAG (Directed Acyclic Graph)

A workflow has a **name** and a **DAG** made of **nodes**:

- **Node**: `id`, `handler`, `dependencies` (list of node IDs), optional `config`.
- **Handlers**:
  - **input**: Entry point; no worker. Orchestrator completes it with `node_params` you send at trigger time.
  - **call_external_service**: Mock HTTP call (sleep 1–2 s, return dummy JSON). Config: `url`.
  - **llm_service**: Mock LLM (sleep, return mock text). Config: `prompt` (can use `{{ node_id.key }}`).
  - **output**: Aggregates all dependency outputs into one result.

**Example (linear):** `input → call_external_service → output`  
**Example (fan-out/fan-in):** `input → get_user, get_posts → output` (get_user and get_posts run in parallel).

### 2. Execution Lifecycle

1. **POST /workflow** — Submit DAG → get `execution_id`. State is **PENDING**.
2. **POST /workflow/trigger/{execution_id}** — Start run. Optional body: `{"node_params": {...}}` for the **input** node. State becomes **RUNNING**; root nodes (e.g. input) are dispatched.
3. **Input** is completed by the orchestrator; then nodes that depend only on input (e.g. get_user, get_posts) become **ready** and are sent to Kafka. Workers run them and publish results.
4. Orchestrator receives each result, updates Redis, and dispatches the next ready nodes. This repeats until all nodes are COMPLETED (or one FAILED).
5. **GET /workflows/{execution_id}** — Returns current **state** and per-node status (PENDING/RUNNING/COMPLETED/FAILED).
6. **GET /workflows/{execution_id}/results** — Returns **aggregated outputs** of all nodes (useful once workflow is COMPLETED).

### 3. Data Between Nodes

- Each node produces a **JSON output** when it completes.
- Downstream nodes can use that data in their **config** with the syntax: `{{ node_id.output_key }}`.
- Before sending a task to a worker, the **template resolver** replaces these placeholders with real values from completed nodes.

### 4. Race Conditions & Idempotency

- **Fan-in**: If D depends on B and C, B and C might finish at the same time. The orchestrator uses **Redis WATCH** so that “mark D dispatched” and “update B/C state” are atomic — D is only dispatched **once**.
- **Workers** keep an in-memory set of `(execution_id, node_id)` so if the same task is delivered twice (e.g. Kafka redelivery), they do not run it twice.

---

## Project Structure (What Does What)

| Path | Purpose |
|------|--------|
| **app/main.py** | FastAPI app; mounts router; starts/stops Kafka result consumer and Redis. |
| **app/api/routes.py** | Defines POST /workflow, POST /workflow/trigger/:id, GET /workflows/:id, GET /workflows/:id/results. |
| **app/models/schemas.py** | Pydantic models: requests (WorkflowCreateRequest, TriggerRequest), responses, NodeState, WorkflowState, TaskMessage, ResultMessage, etc. |
| **app/services/dag_validator.py** | Validates DAG: no duplicate IDs, all dependencies exist, no cycles (Kahn’s algorithm). |
| **app/services/state_manager.py** | Redis read/write: save/load execution, update node state, mark dispatched, get ready nodes, collect outputs. Uses WATCH for atomicity. |
| **app/services/template_resolver.py** | Replaces `{{ node_id.key }}` in config with values from completed nodes. |
| **app/services/orchestrator.py** | Dispatches ready nodes to Kafka (with resolved config), handles “input” in-process; runs background consumer for **workflow.results** and calls `dispatch_ready_nodes` on each result. |
| **app/handlers.py** | Mock handlers: call_external_service, llm_service, output. Used by workers. |
| **app/workers/task_worker.py** | Kafka consumer for **workflow.tasks**; runs handler, publishes **ResultMessage** to **workflow.results**; idempotency by (execution_id, node_id). |
| **app/core/config.py** | Redis URL, Kafka brokers, topic names from env. |
| **app/core/redis_client.py** | Async Redis connection pool. |
| **app/core/kafka_client.py** | Kafka producer and consumer helpers. |

---

## API Flow (Order to Call in Postman)

1. **GET /health**  
   Check that the API is up.

2. **POST /workflow**  
   Body: `{ "name": "...", "dag": { "nodes": [ ... ] } }`.  
   Copy the returned **execution_id** for the next steps.

3. **POST /workflow/trigger/{{execution_id}}**  
   Optional body: `{ "node_params": { "key": "value" } }`.  
   This starts the run. Response is immediate; work continues in the background.

4. **GET /workflows/{{execution_id}}**  
   Call repeatedly until `state` is **COMPLETED** or **FAILED**.  
   You’ll see each node move from PENDING → RUNNING → COMPLETED.

5. **GET /workflows/{{execution_id}}/results**  
   Once COMPLETED, this returns the aggregated outputs of all nodes.

Use the **execution_id** from step 2 in steps 3, 4, and 5 (e.g. in Postman via a collection variable).

---

## Running the Project

```bash
docker compose up --build -d
```

This starts Redis, Kafka, the API (with orchestrator), and workers. API is at **http://localhost:8000**.

- **Health:** http://localhost:8000/health  
- **OpenAPI docs:** http://localhost:8000/docs  

Import the provided Postman collection, set the base URL to `http://localhost:8000`, then run the requests in the order above to test the full flow.
