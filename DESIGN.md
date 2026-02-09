# Design Document — Workflow Engine

This document captures the key design decisions, trade-offs, and rationale behind the Event-Driven Workflow Orchestration Engine.

---

## 1. Overall Architecture

The system follows a **producer–consumer** pattern with clear separation of concerns:

| Layer | Responsibility |
|-------|---------------|
| **API Layer** (FastAPI) | Accepts workflow definitions, validates DAGs, persists initial state, triggers execution, and serves status/results. Responds synchronously. |
| **Orchestrator** (background task inside the API process) | Consumes result messages from Kafka, advances the DAG by dispatching newly-ready nodes, and updates workflow state in Redis. |
| **Workers** (separate processes) | Consume task messages from Kafka, execute handler logic (mocked), and publish result messages back to Kafka. Stateless and horizontally scalable. |
| **Redis** | Single source of truth for workflow execution state. Provides atomic optimistic locking via WATCH/MULTI/EXEC for safe concurrent updates. |
| **Kafka** | Durable, ordered message broker connecting the orchestrator to workers via two topics (`workflow.tasks` and `workflow.results`). |

This design ensures that the API remains responsive (synchronous acknowledgment) while all heavy lifting happens asynchronously through Kafka-mediated message passing.

---

## 2. DAG Validation

Before any execution begins, the submitted JSON graph undergoes three validation checks:

1. **Uniqueness** — No two nodes may share the same `id`.
2. **Referential Integrity** — Every node ID listed in a `dependencies` array must correspond to an existing node in the graph.
3. **Acyclicity** — Kahn's algorithm (topological sort) is used to detect cycles. If the number of nodes processed by the sort is less than the total node count, a cycle exists and the workflow is rejected with a 400 error.

The cycle detection implementation uses a recursive drain function over a `deque` rather than a `while` loop, satisfying the project constraint.

---

## 3. Detecting Readiness & Dispatching

A node is considered **ready** when:
- Its state is `PENDING`.
- It has not been previously dispatched (idempotency guard).
- All of its dependencies are in the `COMPLETED` state.

The orchestrator calls `get_ready_nodes()` after every result message and dispatches all eligible nodes in a single pass. This naturally supports **fan-out**: when a root node completes, all its direct children become ready simultaneously and are dispatched in parallel to Kafka.

---

## 4. Handling Fan-In and Race Conditions

**Fan-in** occurs when a node depends on multiple predecessors (e.g., node D depends on B and C). The challenge is ensuring D is dispatched **exactly once**, even if B and C complete at the same instant.

The solution uses **Redis optimistic locking** (`WATCH` / `MULTI` / `EXEC`):

1. The orchestrator watches the workflow key before reading state.
2. It updates the completed node's state and recomputes the workflow-level state.
3. The `MULTI/EXEC` transaction commits only if no other process modified the key in the meantime.
4. After a successful commit, `dispatch_ready_nodes()` is called, which uses `mark_node_dispatched()` — another atomic WATCH/MULTI/EXEC operation that sets `dispatched = True` on the node. If a second concurrent call tries to dispatch the same node, it sees `dispatched = True` and skips it.

This two-layer atomic guard (state update + dispatch flag) ensures that even under extreme concurrency, each node is dispatched exactly once.

---

## 5. Data Passing & Template Resolution

Nodes produce JSON outputs upon completion. Downstream nodes can reference these outputs using the `{{ node_name.output_key }}` template syntax inside their `config` dictionaries.

The template resolver:
- Supports **full-match replacement** (preserves the original data type, e.g., an integer stays an integer).
- Supports **inline replacement** (multiple references embedded in a larger string are stringified).
- Recursively resolves nested dicts and lists.
- Leaves unresolvable references intact (graceful degradation).

Resolution happens **just before dispatch**, ensuring the freshest data is injected.

---

## 6. Worker Design

Workers are **stateless** Kafka consumers. Each worker:

1. Reads a `TaskMessage` from the `workflow.tasks` topic.
2. Looks up the appropriate handler function based on `handler` field.
3. Executes the handler (mocked — no real external calls).
4. Publishes a `ResultMessage` to the `workflow.results` topic.

**Handler types:**

| Handler | Behavior |
|---------|----------|
| `input` | Handled directly by the orchestrator (never reaches a worker). Immediately completes with `node_params` as output. |
| `call_external_service` | Sleeps 1–2 seconds to simulate an HTTP call, returns a dummy JSON response including the configured URL. |
| `llm_service` | Sleeps 0.5 seconds, returns a mock AI-generated text string incorporating the prompt from config. |
| `output` | Aggregation node — merges all dependency outputs into a single `aggregated` dict. |

**Idempotency** is enforced via an in-memory set of processed `(execution_id, node_id)` keys. If a duplicate message arrives (e.g., due to Kafka redelivery), the worker skips execution and does not publish a duplicate result.

---

## 7. State Management

All workflow state lives in Redis as a single JSON document per execution, keyed by `workflow:<execution_id>`. This simplifies reads (one `GET`) and enables atomic updates via `WATCH`.

Node states follow this lifecycle:

```
PENDING  →  RUNNING  →  COMPLETED
                    →  FAILED
```

The workflow-level state is derived from individual node states:
- **FAILED** if any node is FAILED.
- **COMPLETED** if all nodes are COMPLETED.
- **RUNNING** if at least one node is RUNNING or COMPLETED (but not all).
- **PENDING** otherwise.

---

## 8. Kafka Topic Design

Two topics are used:

| Topic | Direction | Partitions | Purpose |
|-------|-----------|------------|---------|
| `workflow.tasks` | Orchestrator → Workers | 3 | Distributes task messages across worker instances for parallel processing. |
| `workflow.results` | Workers → Orchestrator | 3 | Returns completion/failure signals back to the orchestrator. |

Messages are keyed by `execution_id` to ensure ordering within a single workflow while allowing different workflows to be processed in parallel across partitions.

---

## 9. Trade-offs & Alternatives Considered

| Decision | Alternative | Rationale |
|----------|-------------|-----------|
| Redis for state | PostgreSQL | Redis provides sub-millisecond reads and built-in optimistic locking. PostgreSQL would offer durability and SQL queries but adds latency and complexity for this use case. |
| Single JSON doc per execution | Hash fields per node | A single document simplifies atomic reads/writes. For very large DAGs (1000+ nodes), per-node keys with Lua scripts would scale better. |
| Kafka (KRaft) | Redis Streams | Kafka provides durable, partitioned, replayable message delivery with consumer groups — ideal for distributed workers. Redis Streams would reduce infrastructure but lack Kafka's ecosystem and durability guarantees. |
| In-memory idempotency set | Redis-based dedup | Sufficient for the current scope. For production with worker restarts, a Redis set or Kafka's exactly-once semantics would be more robust. |
| Recursive queue drain | While loop | Project constraint. The recursive approach is functionally equivalent for DAGs of reasonable size. |

---

## 10. Scalability Considerations

- **Workers** scale horizontally — add more replicas in `docker-compose.yml` or via Kubernetes.
- **Kafka partitions** can be increased to improve throughput.
- **Redis** can be replaced with a Redis Cluster for high-availability.
- **API** can be load-balanced behind Nginx or an ingress controller; the orchestrator's result consumer should run as a singleton (or use Kafka consumer groups to distribute across API replicas).
