"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
WORKFLOW_KEY_PREFIX: str = "workflow:"

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TASK_TOPIC: str = os.getenv("KAFKA_TASK_TOPIC", "workflow.tasks")
KAFKA_RESULT_TOPIC: str = os.getenv("KAFKA_RESULT_TOPIC", "workflow.results")
KAFKA_TASK_GROUP: str = os.getenv("KAFKA_TASK_GROUP", "worker-group")
KAFKA_RESULT_GROUP: str = os.getenv("KAFKA_RESULT_GROUP", "orchestrator-group")
