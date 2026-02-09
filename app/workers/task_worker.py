"""Task Worker – consumes from the Kafka task topic, executes handlers,
and publishes results back to the Kafka result topic.

Handler implementations live in ``app.handlers``. Input nodes are handled
by the orchestrator and never reach the worker.
"""

from __future__ import annotations

import logging
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.core.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_RESULT_TOPIC,
    KAFKA_TASK_GROUP,
    KAFKA_TASK_TOPIC,
)
from app.handlers import HANDLERS
from app.models.schemas import NodeState, ResultMessage, TaskMessage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory idempotency set (per worker instance)
# ---------------------------------------------------------------------------
_processed_tasks: set[str] = set()


def _idempotency_key(msg: TaskMessage) -> str:
    return f"{msg.execution_id}:{msg.node_id}"


# ---------------------------------------------------------------------------
# Task processing
# ---------------------------------------------------------------------------

async def _process_task(raw_value: str, producer: AIOKafkaProducer) -> None:
    """Deserialize, execute, and publish result for a single task message."""
    task = TaskMessage.model_validate_json(raw_value)
    idem_key = _idempotency_key(task)

    # Idempotency check
    if idem_key in _processed_tasks:
        logger.info("Duplicate task %s – skipping.", idem_key)
        return
    _processed_tasks.add(idem_key)

    handler_fn = HANDLERS.get(task.handler)
    if handler_fn is None:
        logger.error("Unknown handler '%s' for task %s", task.handler, idem_key)
        result = ResultMessage(
            execution_id=task.execution_id,
            node_id=task.node_id,
            status=NodeState.FAILED,
            error=f"Unknown handler: {task.handler}",
        )
    else:
        try:
            output = await handler_fn(task)
            result = ResultMessage(
                execution_id=task.execution_id,
                node_id=task.node_id,
                status=NodeState.COMPLETED,
                output=output,
            )
        except Exception as exc:
            logger.exception("Handler '%s' failed for %s", task.handler, idem_key)
            result = ResultMessage(
                execution_id=task.execution_id,
                node_id=task.node_id,
                status=NodeState.FAILED,
                error=str(exc),
            )

    await producer.send_and_wait(
        KAFKA_RESULT_TOPIC,
        key=task.execution_id,
        value=result.model_dump_json(),
    )
    logger.info("Published result for %s -> %s", idem_key, result.status)


# ---------------------------------------------------------------------------
# Main worker entry-point (standalone process)
# ---------------------------------------------------------------------------

async def run_worker() -> None:
    """Start the Kafka consumer loop for task processing.

    Uses ``async for`` over the consumer (no while loop).
    """
    consumer = AIOKafkaConsumer(
        KAFKA_TASK_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_TASK_GROUP,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    await consumer.start()
    await producer.start()
    logger.info("Worker started – listening on topic '%s'", KAFKA_TASK_TOPIC)

    try:
        async for msg in consumer:
            try:
                await _process_task(msg.value, producer)
            except Exception:
                logger.exception("Unhandled error processing message: %s", msg.value)
    finally:
        await consumer.stop()
        await producer.stop()
