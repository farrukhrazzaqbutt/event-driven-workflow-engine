"""Mock handlers for workflow nodes. No real external calls.

Used by the task worker to execute node types:
  * call_external_service – mock HTTP call (sleep 1–2 s, return dummy JSON).
  * llm_service         – mock LLM call (prompt in config, returns mock text).
  * output              – aggregation; merges dependency outputs.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any, Dict

from app.models.schemas import TaskMessage


async def handle_call_external_service(msg: TaskMessage) -> Dict[str, Any]:
    """Mock external HTTP call – sleep 1–2 s and return dummy data."""
    url = msg.config.get("url", "http://mock-service/default")
    delay = random.uniform(1.0, 2.0)
    await asyncio.sleep(delay)
    return {
        "source": url,
        "status_code": 200,
        "body": {"mock": True, "node": msg.node_id, "fetched_from": url},
    }


async def handle_llm_service(msg: TaskMessage) -> Dict[str, Any]:
    """Mock LLM generation – accept a prompt (with resolved vars) and return mock text."""
    prompt = msg.config.get("prompt", "No prompt provided.")
    await asyncio.sleep(0.5)
    return {
        "generated_text": f"[Mock LLM Response] Based on prompt: '{prompt}' — "
                          f"Here is a simulated AI-generated answer for node '{msg.node_id}'.",
    }


async def handle_output(msg: TaskMessage) -> Dict[str, Any]:
    """Aggregation node – merge all dependency outputs into a single result."""
    aggregated: Dict[str, Any] = {}
    for dep_id, dep_output in msg.resolved_inputs.items():
        aggregated[dep_id] = dep_output
    return {"aggregated": aggregated}


HANDLERS: Dict[str, Any] = {
    "call_external_service": handle_call_external_service,
    "llm_service": handle_llm_service,
    "output": handle_output,
}
