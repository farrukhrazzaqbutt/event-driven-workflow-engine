"""Standalone entry-point for the Kafka task worker process."""

from __future__ import annotations

import asyncio
import logging

from app.workers.task_worker import run_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
