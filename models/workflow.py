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
    step_id: Optional[str] = None  # Assigned by scheduler when job is submitted
    
    # Execution tracking fields
    status: Optional[str] = "pending"
    task_id: Optional[str] = None  # Scheduler task ID (same as step_id when submitted)
    submitted_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    elapsed_time: Optional[str] = None
    error_message: Optional[str] = None
    
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


class ExecutionMetadata(BaseModel):
    """Workflow execution tracking metadata."""
    total_steps: int = 0
    completed_steps: int = 0
    running_steps: int = 0
    failed_steps: int = 0
    pending_steps: int = 0
    currently_running_step_ids: List[str] = Field(default_factory=list)
    completed_step_ids: List[str] = Field(default_factory=list)
    max_parallel_steps: int = 2
    
    class Config:
        extra = "allow"


class WorkflowSubmission(BaseModel):
    """Workflow with scheduler-assigned IDs (stored format)."""
    workflow_id: str
    workflow_name: str
    version: str
    base_context: BaseContext
    steps: List[WorkflowStep]
    workflow_outputs: Optional[List[str]] = None
    
    # Status and timestamps
    status: str = "pending"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Execution tracking
    execution_metadata: Optional[ExecutionMetadata] = None
    auth_token: Optional[str] = None  # Stored plaintext for now (TODO: encrypt)
    log_file_path: Optional[str] = None
    error_message: Optional[str] = None
    
    class Config:
        extra = "allow"


class WorkflowStatusEnum(str, Enum):
    """Workflow status enumeration."""
    PENDING = "pending"          # Created but not yet picked up by executor
    QUEUED = "queued"            # Picked up by executor, waiting to start
    RUNNING = "running"          # Currently executing steps
    SUCCEEDED = "succeeded"      # All steps completed successfully
    FAILED = "failed"            # One or more steps failed
    CANCELLED = "cancelled"      # User cancelled the workflow


class StepStatusEnum(str, Enum):
    """Step status enumeration."""
    PENDING = "pending"          # Waiting for dependencies
    READY = "ready"              # Dependencies met, ready to submit
    QUEUED = "queued"            # Submitted to scheduler, waiting to start
    RUNNING = "running"          # Currently executing
    SUCCEEDED = "succeeded"      # Completed successfully
    FAILED = "failed"            # Execution failed
    SKIPPED = "skipped"          # Skipped (future use)
    UPSTREAM_FAILED = "upstream_failed"  # Dependency failed


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

