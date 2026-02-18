"""API route handlers."""
import json
from typing import Dict, Any, Optional
from fastapi import APIRouter, HTTPException, status, Header
from pydantic import BaseModel, Field

from core.workflow_manager import WorkflowManager
from models.workflow import WorkflowStatus
from utils.logger import get_logger


logger = get_logger(__name__)
router = APIRouter()

# Global workflow manager instance
workflow_manager: WorkflowManager = None


def _sanitize_incoming_workflow_payload(workflow_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize incoming workflow payloads before schema validation/submission.

    Accepts either:
    - Raw workflow manifest
    - Wrapper payload containing {"workflow_json": {...}, ...}

    Strips helper metadata fields that are not part of WorkflowDefinition.
    """
    payload = workflow_data if isinstance(workflow_data, dict) else {}

    # Support wrapper payloads from planner responses.
    if isinstance(payload.get("workflow_json"), dict):
        payload = payload["workflow_json"]
        logger.info("Unwrapped workflow_json wrapper from request payload")

    cleaned = dict(payload)

    # Tolerate planner metadata without failing strict schema validation.
    if "workflow_description" in cleaned:
        logger.info("Stripping non-schema field 'workflow_description' from workflow payload")
        cleaned.pop("workflow_description", None)

    return cleaned


def set_workflow_manager(manager: WorkflowManager):
    """Set the workflow manager instance."""
    global workflow_manager
    workflow_manager = manager


class SubmitResponse(BaseModel):
    """Response model for workflow submission."""
    workflow_id: str
    status: str
    message: str = "Workflow submitted successfully"


class PlanResponse(BaseModel):
    """Response model for workflow planning."""
    workflow_id: str
    status: str
    workflow_name: str
    step_count: int
    message: str = "Workflow planned successfully"


class RegisterResponse(BaseModel):
    """Response model for workflow registration."""
    workflow_id: str
    status: str
    workflow_name: str
    step_count: int
    message: str = "Workflow registered successfully"


class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str
    mongodb: str
    version: str = "1.0.0"


class ValidateResponse(BaseModel):
    """Response model for workflow validation."""
    valid: bool = True
    workflow_json: Dict[str, Any]
    warnings: list[str] = Field(default_factory=list)
    auto_fixes: list[str] = Field(default_factory=list)
    message: str = "Workflow validated successfully"


@router.post(
    "/workflows/register",
    response_model=RegisterResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a workflow",
    description="Validate, assign workflow_id, and persist a workflow with planned status."
)
async def register_workflow(
    workflow_data: Dict[str, Any],
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> RegisterResponse:
    """Register and persist a new workflow without execution side effects."""
    try:
        logger.info("Received workflow registration request")
        workflow_data = _sanitize_incoming_workflow_payload(workflow_data)
        logger.info(
            "Full workflow registration data:\n%s",
            json.dumps(workflow_data, indent=2)
        )

        auth_token = authorization
        result = workflow_manager.register_workflow(workflow_data, auth_token=auth_token)

        return RegisterResponse(
            workflow_id=result["workflow_id"],
            status=result["status"],
            workflow_name=result["workflow_name"],
            step_count=result["step_count"]
        )
    except ValueError as e:
        logger.error(f"Workflow registration validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Workflow registration failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.post(
    "/workflows/plan",
    response_model=PlanResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Plan a workflow",
    description="Validate and persist a workflow plan without submitting it for execution."
)
async def plan_workflow(
    workflow_data: Dict[str, Any],
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> PlanResponse:
    """Plan and persist a new workflow without execution side effects."""
    try:
        logger.info("Received workflow planning request")
        workflow_data = _sanitize_incoming_workflow_payload(workflow_data)
        logger.info(
            "Full workflow planning data:\n%s",
            json.dumps(workflow_data, indent=2)
        )

        auth_token = authorization
        result = workflow_manager.plan_workflow(workflow_data, auth_token=auth_token)

        return PlanResponse(
            workflow_id=result["workflow_id"],
            status=result["status"],
            workflow_name=result["workflow_name"],
            step_count=result["step_count"]
        )
    except ValueError as e:
        logger.error(f"Workflow planning validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Workflow planning failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.post(
    "/workflows/{workflow_id}/submit",
    response_model=SubmitResponse,
    status_code=status.HTTP_200_OK,
    summary="Submit a planned workflow",
    description="Promote an existing planned workflow to pending execution."
)
async def submit_planned_workflow(
    workflow_id: str,
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> SubmitResponse:
    """Submit an existing planned workflow by ID."""
    try:
        logger.info(f"Received planned workflow submission request for {workflow_id}")
        auth_token = authorization
        result = workflow_manager.submit_planned_workflow(workflow_id, auth_token=auth_token)
        return SubmitResponse(
            workflow_id=result['workflow_id'],
            status=result['status']
        )
    except ValueError as e:
        logger.error(f"Planned workflow submission validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Planned workflow submission failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.post(
    "/workflows/submit",
    response_model=SubmitResponse,
    status_code=status.HTTP_200_OK,
    summary="Submit a registered workflow",
    description="Submit a previously validated/registered workflow for execution. "
                "Request payload must include an existing workflow_id."
)
async def submit_workflow(
    workflow_data: Dict[str, Any],
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> SubmitResponse:
    """Submit a previously registered workflow.

    Args:
        workflow_data: Payload containing workflow_id
        authorization: Optional authorization token in Authorization header

    Returns:
        Submission response with workflow_id and pending status

    Raises:
        HTTPException: 400 for validation errors, 500 for server errors
    """
    try:
        logger.info("Received workflow submission request")
        workflow_data = _sanitize_incoming_workflow_payload(workflow_data)

        # Log the full incoming workflow data for debugging
        logger.info(
            f"Full workflow submission data:\n{json.dumps(workflow_data, indent=2)}"
        )

        # Extract auth token from Authorization header if provided
        auth_token = authorization

        result = workflow_manager.submit_workflow(workflow_data, auth_token=auth_token)

        return SubmitResponse(
            workflow_id=result['workflow_id'],
            status=result['status']
        )

    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Workflow submission failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.post(
    "/workflows/validate",
    response_model=ValidateResponse,
    status_code=status.HTTP_200_OK,
    summary="Validate a workflow",
    description="Validate and normalize a workflow without submitting or persisting it. "
                "Authorization token should be provided in the Authorization header "
                "when workspace-aware checks are required."
)
async def validate_workflow(
    workflow_data: Dict[str, Any],
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> ValidateResponse:
    """Validate a workflow without submission side effects.

    Args:
        workflow_data: Workflow JSON dictionary
        authorization: Optional authorization token in Authorization header

    Returns:
        Validation response including normalized workflow_json

    Raises:
        HTTPException: 400 for validation errors, 500 for server errors
    """
    try:
        logger.info("Received workflow validation request")
        workflow_data = _sanitize_incoming_workflow_payload(workflow_data)
        logger.info(
            "Full workflow validation data (pre-validation):\n%s",
            json.dumps(workflow_data, indent=2)
        )
        auth_token = authorization

        result = workflow_manager.validate_workflow(
            workflow_data,
            auth_token=auth_token
        )

        return ValidateResponse(
            valid=result.get("valid", True),
            workflow_json=result.get("workflow_json", {}),
            warnings=result.get("warnings", []),
            auto_fixes=result.get("auto_fixes", []),
            message=result.get("message", "Workflow validated successfully")
        )

    except ValueError as e:
        logger.error(f"Workflow validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Workflow validation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/workflows/{workflow_id}/status",
    response_model=WorkflowStatus,
    summary="Get workflow status",
    description="Retrieve the current status of a workflow by its ID."
)
async def get_workflow_status(workflow_id: str) -> WorkflowStatus:
    """Get status of a workflow.

    Args:
        workflow_id: Workflow identifier

    Returns:
        Workflow status information

    Raises:
        HTTPException: 404 if not found, 500 for server errors
    """
    try:
        logger.info(f"Received status request for workflow {workflow_id}")

        status_info = workflow_manager.get_workflow_status(workflow_id)

        return status_info

    except ValueError as e:
        logger.error(f"Workflow not found: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error retrieving workflow status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/workflows/{workflow_id}",
    summary="Get full workflow",
    description="Retrieve the complete workflow document including all details."
)
async def get_workflow(workflow_id: str) -> Dict[str, Any]:
    """Get complete workflow document.

    Args:
        workflow_id: Workflow identifier

    Returns:
        Complete workflow dictionary

    Raises:
        HTTPException: 404 if not found, 500 for server errors
    """
    try:
        logger.info(f"Received full workflow request for {workflow_id}")

        workflow = workflow_manager.get_full_workflow(workflow_id)

        return workflow

    except ValueError as e:
        logger.error(f"Workflow not found: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error retrieving workflow: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.post(
    "/workflows/submit-cwl",
    response_model=SubmitResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a CWL workflow",
    description="Submit a CWL (Common Workflow Language) workflow for execution. "
                "The CWL workflow will be converted to the internal format and "
                "submitted. Authorization token should be provided in the "
                "Authorization header for scheduler API calls."
)
async def submit_cwl_workflow(
    workflow_data: Dict[str, Any],
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> SubmitResponse:
    """Submit a CWL workflow.

    Args:
        workflow_data: CWL workflow dictionary (YAML or JSON format)
        authorization: Optional authorization token in Authorization header

    Returns:
        Submission response with workflow_id

    Raises:
        HTTPException: 400 for validation/conversion errors, 500 for server errors
    """
    try:
        logger.info("Received CWL workflow submission request")

        # Extract auth token from Authorization header if provided
        auth_token = authorization

        result = workflow_manager.submit_cwl_workflow(workflow_data, auth_token=auth_token)

        return SubmitResponse(
            workflow_id=result['workflow_id'],
            status=result['status'],
            message="CWL workflow converted and submitted successfully"
        )

    except ValueError as e:
        logger.error(f"CWL workflow conversion/validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"CWL workflow submission failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.post(
    "/workflows/{workflow_id}/cancel",
    summary="Cancel a workflow",
    description="Cancel a running or pending workflow. The executor will stop "
                "submitting new steps and mark the workflow as cancelled."
)
async def cancel_workflow(workflow_id: str) -> Dict[str, Any]:
    """Cancel a workflow.

    Args:
        workflow_id: Workflow identifier

    Returns:
        Cancellation confirmation

    Raises:
        HTTPException: 404 if not found, 400 if already completed, 500 for errors
    """
    try:
        logger.info(f"Received cancellation request for workflow {workflow_id}")

        # Get workflow
        workflow = workflow_manager.get_full_workflow(workflow_id)

        current_status = workflow.get('status')

        # Check if workflow is already in terminal state
        if current_status in ['succeeded', 'failed', 'cancelled']:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot cancel workflow with status '{current_status}'"
            )

        # Update status to cancelled
        workflow_manager.update_workflow_status(workflow_id, 'cancelled')

        logger.info(f"Workflow {workflow_id} marked as cancelled")

        return {
            "workflow_id": workflow_id,
            "status": "cancelled",
            "message": "Workflow cancellation requested. Executor will stop processing."
        }

    except ValueError as e:
        logger.error(f"Workflow not found: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling workflow: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Check the health status of the workflow engine and its dependencies."
)
async def health_check() -> HealthResponse:
    """Health check endpoint.

    Returns:
        Health status information
    """
    try:
        # Test MongoDB connection
        workflow_manager.state_manager.client.admin.command('ping')
        mongodb_status = "connected"
    except Exception as e:
        logger.error(f"MongoDB health check failed: {e}")
        mongodb_status = "disconnected"

    return HealthResponse(
        status="healthy" if mongodb_status == "connected" else "degraded",
        mongodb=mongodb_status
    )

