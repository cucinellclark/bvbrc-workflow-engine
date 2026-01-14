"""API route handlers."""
from typing import Dict, Any, Optional
from fastapi import APIRouter, HTTPException, status, Header
from pydantic import BaseModel

from core.workflow_manager import WorkflowManager
from models.workflow import WorkflowStatus
from utils.logger import get_logger


logger = get_logger(__name__)
router = APIRouter()

# Global workflow manager instance
workflow_manager: WorkflowManager = None


def set_workflow_manager(manager: WorkflowManager):
    """Set the workflow manager instance."""
    global workflow_manager
    workflow_manager = manager


class SubmitResponse(BaseModel):
    """Response model for workflow submission."""
    workflow_id: str
    status: str
    message: str = "Workflow submitted successfully"


class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str
    mongodb: str
    version: str = "1.0.0"


@router.post(
    "/workflows/submit",
    response_model=SubmitResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a new workflow",
    description="Submit a workflow for execution. The workflow JSON should not "
                "contain workflow_id or step_id fields as these are assigned by "
                "the scheduler. Authorization token should be provided in the "
                "Authorization header for scheduler API calls."
)
async def submit_workflow(
    workflow_data: Dict[str, Any],
    authorization: Optional[str] = Header(None, alias="Authorization")
) -> SubmitResponse:
    """Submit a new workflow.
    
    Args:
        workflow_data: Workflow JSON dictionary
        authorization: Optional authorization token in Authorization header
        
    Returns:
        Submission response with workflow_id
        
    Raises:
        HTTPException: 400 for validation errors, 500 for server errors
    """
    try:
        logger.info("Received workflow submission request")
        
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

