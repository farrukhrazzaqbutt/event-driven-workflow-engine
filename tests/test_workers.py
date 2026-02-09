"""Tests for worker handler functions."""

import pytest

from app.handlers import (
    handle_call_external_service,
    handle_llm_service,
    handle_output,
)
from app.models.schemas import TaskMessage
from app.workers.task_worker import _idempotency_key


@pytest.mark.asyncio
async def test_call_external_service_handler() -> None:
    msg = TaskMessage(
        execution_id="e1",
        node_id="n1",
        handler="call_external_service",
        config={"url": "http://example.com/api"},
        resolved_inputs={},
    )
    result = await handle_call_external_service(msg)
    assert result["source"] == "http://example.com/api"
    assert result["status_code"] == 200
    assert result["body"]["mock"] is True


@pytest.mark.asyncio
async def test_llm_service_handler() -> None:
    msg = TaskMessage(
        execution_id="e1",
        node_id="n1",
        handler="llm_service",
        config={"prompt": "Tell me about Python"},
        resolved_inputs={},
    )
    result = await handle_llm_service(msg)
    assert "Mock LLM Response" in result["generated_text"]
    assert "Tell me about Python" in result["generated_text"]


@pytest.mark.asyncio
async def test_output_handler_aggregates() -> None:
    msg = TaskMessage(
        execution_id="e1",
        node_id="output",
        handler="output",
        config={},
        resolved_inputs={
            "get_user": {"name": "Alice"},
            "get_posts": {"count": 5},
        },
    )
    result = await handle_output(msg)
    assert "get_user" in result["aggregated"]
    assert "get_posts" in result["aggregated"]
    assert result["aggregated"]["get_user"]["name"] == "Alice"


def test_idempotency_key() -> None:
    msg = TaskMessage(
        execution_id="exec-1",
        node_id="node-A",
        handler="output",
    )
    assert _idempotency_key(msg) == "exec-1:node-A"
