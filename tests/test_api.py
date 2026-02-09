"""Integration tests for the API endpoints.

These tests mock Redis and Kafka so they can run without infrastructure.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from fastapi.testclient import TestClient

from app.models.schemas import WorkflowExecution, WorkflowState, NodeInstance, NodeState


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _mock_lifespan():
    """Disable the real lifespan (Kafka consumer) during tests."""
    from contextlib import asynccontextmanager
    from typing import AsyncIterator
    from fastapi import FastAPI

    @asynccontextmanager
    async def noop_lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield

    with patch("app.main.lifespan", noop_lifespan):
        # Re-import to pick up the patched lifespan
        from app.main import app
        yield app


@pytest.fixture()
def client(_mock_lifespan) -> TestClient:
    return TestClient(_mock_lifespan)


# ---------------------------------------------------------------------------
# Sample payloads
# ---------------------------------------------------------------------------

VALID_WORKFLOW = {
    "name": "Parallel API Fetcher",
    "dag": {
        "nodes": [
            {"id": "input", "handler": "input", "dependencies": []},
            {
                "id": "get_user",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://localhost:8911/document/policy/list"},
            },
            {
                "id": "get_posts",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://localhost:8911/document/policy/list"},
            },
            {
                "id": "get_comments",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://localhost:8911/document/policy/list"},
            },
            {
                "id": "output",
                "handler": "output",
                "dependencies": ["get_user", "get_posts", "get_comments"],
            },
        ]
    },
}

CYCLIC_WORKFLOW = {
    "name": "Bad",
    "dag": {
        "nodes": [
            {"id": "A", "handler": "input", "dependencies": ["B"]},
            {"id": "B", "handler": "output", "dependencies": ["A"]},
        ]
    },
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCreateWorkflow:
    """POST /workflow"""

    @patch("app.services.state_manager.save_execution", new_callable=AsyncMock)
    def test_create_valid_workflow(self, mock_save: AsyncMock, client: TestClient) -> None:
        resp = client.post("/workflow", json=VALID_WORKFLOW)
        assert resp.status_code == 201
        body = resp.json()
        assert "execution_id" in body
        assert body["message"] == "Workflow registered successfully."
        mock_save.assert_awaited_once()

    def test_create_cyclic_workflow_rejected(self, client: TestClient) -> None:
        resp = client.post("/workflow", json=CYCLIC_WORKFLOW)
        assert resp.status_code == 400
        assert "Cycle detected" in resp.json()["detail"]

    def test_create_missing_dependency(self, client: TestClient) -> None:
        payload = {
            "name": "Bad",
            "dag": {
                "nodes": [
                    {"id": "A", "handler": "input", "dependencies": ["ghost"]},
                ]
            },
        }
        resp = client.post("/workflow", json=payload)
        assert resp.status_code == 400
        assert "unknown node" in resp.json()["detail"]


class TestGetWorkflowStatus:
    """GET /workflows/{execution_id}"""

    @patch("app.services.state_manager.load_execution", new_callable=AsyncMock)
    def test_status_found(self, mock_load: AsyncMock, client: TestClient) -> None:
        mock_load.return_value = WorkflowExecution(
            execution_id="abc-123",
            name="Test",
            state=WorkflowState.RUNNING,
            nodes={
                "A": NodeInstance(id="A", handler="input", state=NodeState.COMPLETED),
                "B": NodeInstance(id="B", handler="output", state=NodeState.RUNNING, dependencies=["A"]),
            },
        )
        resp = client.get("/workflows/abc-123")
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "RUNNING"
        assert body["nodes"]["A"] == "COMPLETED"

    @patch("app.services.state_manager.load_execution", new_callable=AsyncMock)
    def test_status_not_found(self, mock_load: AsyncMock, client: TestClient) -> None:
        mock_load.return_value = None
        resp = client.get("/workflows/nonexistent")
        assert resp.status_code == 404


class TestGetWorkflowResults:
    """GET /workflows/{execution_id}/results"""

    @patch("app.services.state_manager.collect_outputs", new_callable=AsyncMock)
    @patch("app.services.state_manager.load_execution", new_callable=AsyncMock)
    def test_results_completed(
        self,
        mock_load: AsyncMock,
        mock_outputs: AsyncMock,
        client: TestClient,
    ) -> None:
        mock_load.return_value = WorkflowExecution(
            execution_id="abc-123",
            name="Test",
            state=WorkflowState.COMPLETED,
            nodes={
                "A": NodeInstance(id="A", handler="input", state=NodeState.COMPLETED, output={"data": "ok"}),
            },
        )
        mock_outputs.return_value = {"A": {"data": "ok"}}
        resp = client.get("/workflows/abc-123/results")
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "COMPLETED"
        assert body["results"]["A"]["data"] == "ok"


class TestTriggerWorkflow:
    """POST /workflow/trigger/{execution_id}"""

    @patch("app.services.orchestrator.dispatch_ready_nodes", new_callable=AsyncMock)
    @patch("app.services.state_manager.save_execution", new_callable=AsyncMock)
    @patch("app.services.state_manager.load_execution", new_callable=AsyncMock)
    def test_trigger_success(
        self,
        mock_load: AsyncMock,
        mock_save: AsyncMock,
        mock_dispatch: AsyncMock,
        client: TestClient,
    ) -> None:
        mock_load.return_value = WorkflowExecution(
            execution_id="abc-123",
            name="Test",
            state=WorkflowState.PENDING,
            nodes={
                "A": NodeInstance(id="A", handler="input"),
            },
        )
        resp = client.post("/workflow/trigger/abc-123", json={"node_params": {"key": "val"}})
        assert resp.status_code == 200
        body = resp.json()
        assert body["execution_id"] == "abc-123"
        mock_dispatch.assert_awaited_once()

    @patch("app.services.state_manager.load_execution", new_callable=AsyncMock)
    def test_trigger_not_found(self, mock_load: AsyncMock, client: TestClient) -> None:
        mock_load.return_value = None
        resp = client.post("/workflow/trigger/nonexistent")
        assert resp.status_code == 404

    @patch("app.services.state_manager.load_execution", new_callable=AsyncMock)
    def test_trigger_already_running(self, mock_load: AsyncMock, client: TestClient) -> None:
        mock_load.return_value = WorkflowExecution(
            execution_id="abc-123",
            name="Test",
            state=WorkflowState.RUNNING,
            nodes={},
        )
        resp = client.post("/workflow/trigger/abc-123")
        assert resp.status_code == 409
