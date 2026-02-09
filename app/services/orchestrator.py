"""Orchestrator – the brain of the workflow engine.

Responsibilities:
  * Dispatch ready nodes to the Kafka task topic.
  * Consume results from the Kafka result topic and advance the DAG.
  * Resolve templates before dispatching.
  * Handle fan-out / fan-in and race conditions atomically.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import List

from aiokafka import AIOKafkaConsumer

from app.core.config import (
    KAFKA_RESULT_GROUP,
    KAFKA_RESULT_TOPIC,
    KAFKA_TASK_TOPIC,
)
from app.core.kafka_client import create_consumer, get_producer
from app.models.schemas import (
    NodeState,
    ResultMessage,
    TaskMessage,
    WorkflowState,
)
from app.services import state_manager, template_resolver

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dispatch helpers
# ---------------------------------------------------------------------------

async def dispatch_ready_nodes(execution_id: str) -> None:
    """Find all ready nodes for *execution_id* and publish task messages."""
    ready_ids: List[str] = await state_manager.get_ready_nodes(execution_id)

    producer = await get_producer()

    for node_id in ready_ids:
        # Atomically mark as dispatched (idempotency guard)
        dispatched = await state_manager.mark_node_dispatched(execution_id, node_id)
        if not dispatched:
            continue

        execution = await state_manager.load_execution(execution_id)
        if execution is None:
            continue

        node = execution.nodes[node_id]

        # Collect outputs from completed predecessors
        all_outputs = await state_manager.collect_outputs(execution_id)

        # Resolve templates in config
        resolved = template_resolver.resolve_inputs(
            node_config=node.config,
            node_dependencies=node.dependencies,
            all_outputs=all_outputs,
        )

        # For "input" handler, inject node_params as output immediately
        if node.handler == "input":
            await state_manager.update_node_state(
                execution_id,
                node_id,
                NodeState.COMPLETED,
                output=execution.node_params or {"data": "input_received"},
            )
            # Recurse to dispatch newly-ready nodes
            await dispatch_ready_nodes(execution_id)
            return

        task_msg = TaskMessage(
            execution_id=execution_id,
            node_id=node_id,
            handler=node.handler,
            config=resolved.get("config", {}),
            resolved_inputs=resolved.get("dependency_outputs", {}),
        )

        await producer.send_and_wait(
            KAFKA_TASK_TOPIC,
            key=execution_id,
            value=task_msg.model_dump_json(),
        )
        logger.info("Dispatched task %s/%s to Kafka", execution_id, node_id)


# ---------------------------------------------------------------------------
# Result consumer (runs as a background asyncio task)
# ---------------------------------------------------------------------------

_consumer_task: asyncio.Task | None = None


async def _process_result(raw_value: str) -> None:
    """Process a single result message from Kafka."""
    result = ResultMessage.model_validate_json(raw_value)
    logger.info(
        "Received result for %s/%s -> %s",
        result.execution_id,
        result.node_id,
        result.status,
    )

    execution = await state_manager.update_node_state(
        execution_id=result.execution_id,
        node_id=result.node_id,
        new_state=result.status,
        output=result.output,
    )

    if execution is None:
        logger.warning("Execution %s not found, skipping.", result.execution_id)
        return

    # If workflow is still running, dispatch next batch of ready nodes
    if execution.state == WorkflowState.RUNNING:
        await dispatch_ready_nodes(result.execution_id)
    elif execution.state == WorkflowState.COMPLETED:
        logger.info("Workflow %s COMPLETED.", result.execution_id)
    elif execution.state == WorkflowState.FAILED:
        logger.error("Workflow %s FAILED.", result.execution_id)


async def _consume_results(consumer: AIOKafkaConsumer) -> None:
    """Async iterator over Kafka result messages – no while loop."""
    async for msg in consumer:
        try:
            await _process_result(msg.value)
        except Exception:
            logger.exception("Error processing result message: %s", msg.value)


async def start_result_consumer() -> None:
    """Start the background Kafka result consumer."""
    global _consumer_task
    consumer = create_consumer(KAFKA_RESULT_TOPIC, KAFKA_RESULT_GROUP)
    await consumer.start()
    _consumer_task = asyncio.create_task(_consume_results(consumer))
    logger.info("Result consumer started.")


async def stop_result_consumer() -> None:
    """Stop the background result consumer."""
    global _consumer_task
    if _consumer_task is not None:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
        _consumer_task = None
