"""Kafka producer and consumer helpers using aiokafka."""

from __future__ import annotations

import logging
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from app.core.config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

_producer: Optional[AIOKafkaProducer] = None


async def get_producer() -> AIOKafkaProducer:
    """Return (and lazily create) the shared Kafka producer."""
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await _producer.start()
    return _producer


async def close_producer() -> None:
    """Gracefully close the Kafka producer."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


def create_consumer(topic: str, group_id: str) -> AIOKafkaConsumer:
    """Create a new Kafka consumer for the given topic and group."""
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
