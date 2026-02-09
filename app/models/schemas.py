"""Pydantic models for the Workflow Engine API."""

from __future__ import annotations

import enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class NodeState(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WorkflowState(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class NodeDefinition(BaseModel):
    """A single node inside a DAG definition."""
    id: str
    handler: str
    dependencies: List[str] = Field(default_factory=list)
    config: Optional[Dict[str, Any]] = Field(default_factory=dict)


class DAGDefinition(BaseModel):
    """The directed acyclic graph that describes the workflow."""
    nodes: List[NodeDefinition]


class WorkflowCreateRequest(BaseModel):
    """Payload for POST /workflow."""
    name: str
    dag: DAGDefinition


class TriggerRequest(BaseModel):
    """Optional payload for POST /workflow/trigger/:execution_id."""
    node_params: Optional[Dict[str, Any]] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Internal state models (stored in Redis)
# ---------------------------------------------------------------------------

class NodeInstance(BaseModel):
    """Runtime state of a single node within an execution."""
    id: str
    handler: str
    dependencies: List[str] = Field(default_factory=list)
    config: Dict[str, Any] = Field(default_factory=dict)
    state: NodeState = NodeState.PENDING
    output: Optional[Dict[str, Any]] = None
    dispatched: bool = False


class WorkflowExecution(BaseModel):
    """Full runtime state of a workflow execution."""
    execution_id: str
    name: str
    state: WorkflowState = WorkflowState.PENDING
    nodes: Dict[str, NodeInstance] = Field(default_factory=dict)
    node_params: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Kafka message models
# ---------------------------------------------------------------------------

class TaskMessage(BaseModel):
    """Message sent to Kafka task topic for worker consumption."""
    execution_id: str
    node_id: str
    handler: str
    config: Dict[str, Any] = Field(default_factory=dict)
    resolved_inputs: Dict[str, Any] = Field(default_factory=dict)


class ResultMessage(BaseModel):
    """Message sent to Kafka result topic by workers after task completion."""
    execution_id: str
    node_id: str
    status: NodeState
    output: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class WorkflowCreateResponse(BaseModel):
    execution_id: str
    message: str = "Workflow registered successfully."


class TriggerResponse(BaseModel):
    execution_id: str
    message: str = "Workflow execution triggered."


class WorkflowStatusResponse(BaseModel):
    execution_id: str
    name: str
    state: WorkflowState
    nodes: Dict[str, NodeState]


class WorkflowResultsResponse(BaseModel):
    execution_id: str
    state: WorkflowState
    results: Dict[str, Any]
