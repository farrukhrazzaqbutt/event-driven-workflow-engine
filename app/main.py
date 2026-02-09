"""FastAPI application entry-point for the Workflow Engine."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app.api.routes import router
from app.core.kafka_client import close_producer
from app.core.redis_client import close_redis
from app.services.orchestrator import start_result_consumer, stop_result_consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup / shutdown lifecycle hook."""
    logger.info("Starting Workflow Engine API …")
    # Start the background Kafka result consumer
    await start_result_consumer()
    yield
    # Graceful shutdown
    logger.info("Shutting down Workflow Engine API …")
    await stop_result_consumer()
    await close_producer()
    await close_redis()


app = FastAPI(
    title="Workflow Engine",
    description="Event-Driven Workflow Orchestration Engine",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)


@app.get("/health")
async def health() -> dict:
    """Simple health-check endpoint."""
    return {"status": "ok"}
