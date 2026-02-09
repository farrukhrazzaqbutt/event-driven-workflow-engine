"""Workflow state persistence backed by Redis.

Every workflow execution is stored as a single JSON blob under the key
``workflow:<execution_id>``.  All mutations go through this module so that
concurrency is handled via Redis atomic operations.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

from app.core.config import WORKFLOW_KEY_PREFIX
from app.core.redis_client import get_redis
from app.models.schemas import (
    NodeInstance,
    NodeState,
    WorkflowExecution,
    WorkflowState,
)

logger = logging.getLogger(__name__)


def _key(execution_id: str) -> str:
    return f"{WORKFLOW_KEY_PREFIX}{execution_id}"


async def save_execution(execution: WorkflowExecution) -> None:
    """Persist the full execution state to Redis."""
    r: aioredis.Redis = await get_redis()
    await r.set(_key(execution.execution_id), execution.model_dump_json())


async def load_execution(execution_id: str) -> Optional[WorkflowExecution]:
    """Load an execution from Redis.  Returns None if not found."""
    r: aioredis.Redis = await get_redis()
    raw = await r.get(_key(execution_id))
    if raw is None:
        return None
    return WorkflowExecution.model_validate_json(raw)


async def update_node_state(
    execution_id: str,
    node_id: str,
    new_state: NodeState,
    output: Optional[Dict[str, Any]] = None,
) -> Optional[WorkflowExecution]:
    """Atomically update a single node's state (and optional output).

    Uses a Redis WATCH/MULTI/EXEC transaction to avoid race conditions
    when two workers complete at the same instant (Scenario C).

    Returns the updated WorkflowExecution or None on conflict (caller retries).
    """
    r: aioredis.Redis = await get_redis()
    key = _key(execution_id)

    # Optimistic locking via WATCH
    async with r.pipeline(transaction=True) as pipe:
        await pipe.watch(key)
        raw = await pipe.get(key)
        if raw is None:
            await pipe.unwatch()
            return None

        execution = WorkflowExecution.model_validate_json(raw)
        node = execution.nodes.get(node_id)
        if node is None:
            await pipe.unwatch()
            return None

        # Idempotency: skip if already completed/failed
        if node.state in (NodeState.COMPLETED, NodeState.FAILED):
            await pipe.unwatch()
            return execution

        node.state = new_state
        if output is not None:
            node.output = output

        # Recompute workflow-level state
        execution.state = _compute_workflow_state(execution)

        pipe.multi()
        pipe.set(key, execution.model_dump_json())
        await pipe.execute()

    return execution


async def mark_node_dispatched(execution_id: str, node_id: str) -> bool:
    """Atomically mark a node as dispatched.  Returns False if already dispatched (idempotency)."""
    r: aioredis.Redis = await get_redis()
    key = _key(execution_id)

    async with r.pipeline(transaction=True) as pipe:
        await pipe.watch(key)
        raw = await pipe.get(key)
        if raw is None:
            await pipe.unwatch()
            return False

        execution = WorkflowExecution.model_validate_json(raw)
        node = execution.nodes.get(node_id)
        if node is None or node.dispatched:
            await pipe.unwatch()
            return False

        node.dispatched = True
        node.state = NodeState.RUNNING

        # Update workflow state to RUNNING if still PENDING
        if execution.state == WorkflowState.PENDING:
            execution.state = WorkflowState.RUNNING

        pipe.multi()
        pipe.set(key, execution.model_dump_json())
        await pipe.execute()

    return True


def _compute_workflow_state(execution: WorkflowExecution) -> WorkflowState:
    """Derive the overall workflow state from individual node states."""
    states = [n.state for n in execution.nodes.values()]

    if any(s == NodeState.FAILED for s in states):
        return WorkflowState.FAILED
    if all(s == NodeState.COMPLETED for s in states):
        return WorkflowState.COMPLETED
    if any(s in (NodeState.RUNNING, NodeState.COMPLETED) for s in states):
        return WorkflowState.RUNNING
    return WorkflowState.PENDING


async def get_ready_nodes(execution_id: str) -> List[str]:
    """Return node IDs that are PENDING and whose dependencies are all COMPLETED."""
    execution = await load_execution(execution_id)
    if execution is None:
        return []

    ready: List[str] = []
    for node_id, node in execution.nodes.items():
        if node.state != NodeState.PENDING:
            continue
        if node.dispatched:
            continue
        deps_met = all(
            execution.nodes[dep].state == NodeState.COMPLETED
            for dep in node.dependencies
        )
        if deps_met:
            ready.append(node_id)

    return ready


async def collect_outputs(execution_id: str) -> Dict[str, Any]:
    """Gather all node outputs into a single dict keyed by node_id."""
    execution = await load_execution(execution_id)
    if execution is None:
        return {}
    return {
        nid: node.output
        for nid, node in execution.nodes.items()
        if node.output is not None
    }
