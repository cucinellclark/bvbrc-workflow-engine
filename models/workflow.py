"""Workflow data models using Pydantic."""
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from enum import Enum


class BaseContext(BaseModel):
    """Base context for workflow execution."""
    base_url: str
    workspace_output_folder: str
    
    class Config:
        extra = "allow"  # Allow additional fields


class WorkflowStep(BaseModel):
    """Individual workflow step definition."""
    step_name: str
    app: str
    params: Dict[str, Any]
    outputs: Optional[Dict[str, str]] = None
    depends_on: Optional[List[str]] = None
    step_id: Optional[str] = None  # Assigned by scheduler
    
    class Config:
        extra = "forbid"


class WorkflowDefinition(BaseModel):
    """Workflow definition without scheduler-assigned IDs (input format)."""
    workflow_name: str
    version: str
    base_context: BaseContext
    steps: List[WorkflowStep]
    workflow_outputs: Optional[List[str]] = None
    
    @field_validator('steps')
    @classmethod
    def validate_steps_not_empty(cls, v):
        """Ensure steps list is not empty."""
        if not v:
            raise ValueError("Workflow must contain at least one step")
        return v
    
    class Config:
        extra = "forbid"


class WorkflowSubmission(BaseModel):
    """Workflow with scheduler-assigned IDs (stored format)."""
    workflow_id: str
    workflow_name: str
    version: str
    base_context: BaseContext
    steps: List[WorkflowStep]
    workflow_outputs: Optional[List[str]] = None
    status: str = "submitted"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        extra = "allow"


class WorkflowStatusEnum(str, Enum):
    """Workflow status enumeration."""
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(BaseModel):
    """Status information for a single step."""
    step_id: str
    step_name: str
    status: str
    app: str


class WorkflowStatus(BaseModel):
    """Workflow status response."""
    workflow_id: str
    workflow_name: str
    status: str
    created_at: datetime
    updated_at: datetime
    steps: List[StepStatus]
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

