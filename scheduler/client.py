"""Scheduler client with placeholder functions."""
import time
import random
from typing import Dict, Any

from utils.logger import get_logger
from models.workflow import WorkflowDefinition


logger = get_logger(__name__)


class SchedulerClient:
    """Client for interacting with the scheduler.
    
    Currently contains placeholder implementations that generate
    mock IDs. Will be replaced with actual scheduler integration.
    """
    
    def __init__(self, scheduler_url: str = None, timeout: int = 30):
        """Initialize scheduler client.
        
        Args:
            scheduler_url: URL of scheduler service
            timeout: Request timeout in seconds
        """
        self.scheduler_url = scheduler_url
        self.timeout = timeout
        logger.info(
            f"Scheduler client initialized (placeholder mode): "
            f"url={scheduler_url}"
        )
    
    def submit_workflow_to_scheduler(
        self,
        workflow: WorkflowDefinition
    ) -> Dict[str, Any]:
        """Submit workflow to scheduler and get assigned IDs.
        
        PLACEHOLDER: Generates mock IDs. Will be replaced with actual
        scheduler API call.
        
        Args:
            workflow: Validated workflow definition
            
        Returns:
            Workflow dict with scheduler-assigned IDs
        """
        logger.info(
            f"[PLACEHOLDER] Submitting workflow '{workflow.workflow_name}' "
            f"to scheduler"
        )
        
        # Generate workflow ID
        workflow_id = self._generate_workflow_id()
        logger.info(f"[PLACEHOLDER] Generated workflow_id: {workflow_id}")
        
        # Convert workflow to dict
        workflow_dict = workflow.model_dump()
        workflow_dict['workflow_id'] = workflow_id
        
        # Assign step IDs
        for i, step in enumerate(workflow_dict['steps']):
            step_id = self._generate_step_id(i)
            step['step_id'] = step_id
            logger.debug(
                f"[PLACEHOLDER] Assigned step_id {step_id} to "
                f"step '{step['step_name']}'"
            )
        
        # Transform depends_on from step names to step IDs
        self._transform_dependencies(workflow_dict['steps'])
        
        logger.info(
            f"[PLACEHOLDER] Workflow '{workflow.workflow_name}' "
            f"submitted successfully"
        )
        
        return workflow_dict
    
    def get_scheduler_status(self, workflow_id: str) -> Dict[str, Any]:
        """Query scheduler for workflow execution status.
        
        PLACEHOLDER: Returns mock status. Will be replaced with actual
        scheduler API call.
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            Status information from scheduler
        """
        logger.debug(
            f"[PLACEHOLDER] Querying scheduler for workflow {workflow_id}"
        )
        
        # Return mock status
        mock_status = {
            'workflow_id': workflow_id,
            'scheduler_status': 'running',
            'progress': '50%',
            'message': 'Workflow execution in progress'
        }
        
        logger.debug(
            f"[PLACEHOLDER] Scheduler status for {workflow_id}: "
            f"{mock_status['scheduler_status']}"
        )
        
        return mock_status
    
    def cancel_workflow(self, workflow_id: str) -> bool:
        """Request workflow cancellation from scheduler.
        
        PLACEHOLDER: Logs cancellation request. Will be replaced with
        actual scheduler API call.
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            True if cancellation requested successfully
        """
        logger.info(
            f"[PLACEHOLDER] Requesting cancellation of workflow "
            f"{workflow_id} from scheduler"
        )
        
        # In real implementation, would make API call to scheduler
        logger.info(
            f"[PLACEHOLDER] Cancellation request sent for {workflow_id}"
        )
        
        return True
    
    @staticmethod
    def _generate_workflow_id() -> str:
        """Generate mock workflow ID.
        
        Returns:
            Workflow ID string
        """
        timestamp = int(time.time() * 1000)
        random_part = random.randint(1000, 9999)
        return f"wf_{timestamp}_{random_part}"
    
    @staticmethod
    def _generate_step_id(index: int) -> str:
        """Generate mock step ID.
        
        Args:
            index: Step index in workflow
            
        Returns:
            Step ID string
        """
        timestamp = int(time.time() * 1000)
        random_part = random.randint(1000, 9999)
        return f"step_{timestamp}_{index}_{random_part}"
    
    @staticmethod
    def _transform_dependencies(steps: list) -> None:
        """Transform depends_on from step names to step IDs.
        
        Modifies steps in place to replace step name references
        with step ID references in the depends_on arrays.
        
        Args:
            steps: List of step dictionaries with step_id assigned
        """
        # Build mapping from step name to step ID
        name_to_id = {
            step['step_name']: step['step_id']
            for step in steps
        }
        
        # Transform depends_on arrays
        for step in steps:
            if step.get('depends_on'):
                transformed_deps = []
                for dep_name in step['depends_on']:
                    if dep_name in name_to_id:
                        transformed_deps.append(name_to_id[dep_name])
                    else:
                        # This shouldn't happen if validation passed
                        logger.warning(
                            f"Dependency '{dep_name}' not found in "
                            f"step name mapping"
                        )
                        transformed_deps.append(dep_name)
                
                step['depends_on'] = transformed_deps
                logger.debug(
                    f"Transformed dependencies for step "
                    f"'{step['step_name']}': {transformed_deps}"
                )

