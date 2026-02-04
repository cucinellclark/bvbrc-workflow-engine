"""Workflow manager - orchestrates workflow submission and status queries."""
from typing import Dict, Any
from datetime import datetime

from core.validator import WorkflowValidator
from core.state_manager import StateManager
from scheduler.client import SchedulerClient
from models.workflow import WorkflowStatus, StepStatus
from utils.logger import get_logger
from utils.variable_resolver import VariableResolver
from config.config import config
from cwl.converter import CWLConverter
from cwl.parser import CWLParser


logger = get_logger(__name__)


class WorkflowManager:
    """Manages workflow lifecycle: validation, submission, and status tracking."""
    
    def __init__(self):
        """Initialize workflow manager with dependencies."""
        self.validator = WorkflowValidator()
        self.state_manager = StateManager()
        
        scheduler_config = config.scheduler
        self.scheduler_client = SchedulerClient(
            scheduler_url=scheduler_config.get('url'),
            timeout=scheduler_config.get('timeout', 30)
        )
        
        # Initialize CWL converter and parser
        self.cwl_converter = CWLConverter()
        self.cwl_parser = CWLParser()
        
        logger.info("WorkflowManager initialized")
    
    def submit_workflow(self, workflow_json: Dict[str, Any], auth_token: str = None) -> Dict[str, str]:
        """Submit a new workflow.
        
        NEW BEHAVIOR: Does NOT submit to scheduler immediately.
        The workflow executor will pick up pending workflows and submit steps.
        
        Process:
        1. Resolve variable placeholders (e.g., ${workspace_output_folder})
        2. Validate workflow JSON
        3. Generate workflow_id locally
        4. Save to MongoDB with status='pending'
        5. Return workflow ID (executor will pick it up and start execution)
        
        Args:
            workflow_json: Raw workflow JSON dictionary
            auth_token: Optional authorization token for scheduler API calls
            
        Returns:
            Dictionary with workflow_id and status
            
        Raises:
            ValueError: If variable resolution or validation fails
            Exception: If submission fails
        """
        try:
            logger.info("Starting workflow submission")
            
            # Step 1: Resolve variable placeholders
            logger.info("Resolving variable placeholders")
            resolved_workflow = VariableResolver.resolve_workflow_variables(
                workflow_json
            )
            
            # Step 2: Validate workflow
            logger.info("Validating workflow")
            validated_workflow = self.validator.validate_workflow_input(
                resolved_workflow
            )
            
            # Step 3: Generate workflow ID locally (no scheduler call yet)
            workflow_id = self._generate_workflow_id()
            logger.info(f"Generated workflow_id: {workflow_id}")
            
            # Convert to dict and add workflow_id
            workflow_dict = validated_workflow.model_dump()
            workflow_dict['workflow_id'] = workflow_id
            
            # Step 4: Add initial status and timestamps
            workflow_dict['status'] = 'pending'  # Executor will pick this up
            workflow_dict['created_at'] = datetime.utcnow()
            workflow_dict['updated_at'] = datetime.utcnow()
            
            # Initialize step status (all pending, no step_ids yet)
            for step in workflow_dict['steps']:
                if 'status' not in step:
                    step['status'] = 'pending'
            
            # Store auth token (plaintext for now - TODO: encrypt)
            if auth_token:
                workflow_dict['auth_token'] = auth_token
            
            # Initialize execution metadata
            from models.workflow import ExecutionMetadata
            max_parallel = config.executor.get('max_parallel_steps_per_workflow', 3)
            workflow_dict['execution_metadata'] = ExecutionMetadata(
                total_steps=len(workflow_dict['steps']),
                completed_steps=0,
                running_steps=0,
                failed_steps=0,
                pending_steps=len(workflow_dict['steps']),
                currently_running_step_ids=[],
                completed_step_ids=[],
                max_parallel_steps=max_parallel
            ).model_dump()
            
            # Set log file path
            log_dir = config.logging.get('workflow_log_dir', 'logs/workflows')
            workflow_dict['log_file_path'] = f"{log_dir}/{workflow_id}.log"
            
            # Step 5: Save to MongoDB
            logger.info(f"Saving workflow {workflow_id} to database")
            self.state_manager.save_workflow(workflow_dict)
            
            logger.info(
                f"Workflow '{validated_workflow.workflow_name}' submitted "
                f"successfully with ID: {workflow_id} (status=pending, waiting for executor)"
            )
            
            return {
                'workflow_id': workflow_id,
                'status': 'pending'
            }
            
        except ValueError as e:
            logger.error(f"Workflow validation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Workflow submission failed: {e}")
            raise
    
    @staticmethod
    def _generate_workflow_id() -> str:
        """Generate workflow ID locally.
        
        Returns:
            Workflow ID string
        """
        import time
        import random
        timestamp = int(time.time() * 1000)
        random_part = random.randint(1000, 9999)
        return f"wf_{timestamp}_{random_part}"
    
    def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        """Get status of a workflow.
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            WorkflowStatus object
            
        Raises:
            ValueError: If workflow not found
        """
        try:
            logger.info(f"Retrieving status for workflow {workflow_id}")
            
            # Retrieve from database
            workflow = self.state_manager.get_workflow(workflow_id)
            
            if not workflow:
                logger.error(f"Workflow {workflow_id} not found")
                raise ValueError(f"Workflow {workflow_id} not found")
            
            # Optionally query scheduler for real-time status
            # (Currently scheduler returns mock data)
            try:
                scheduler_status = self.scheduler_client.get_scheduler_status(
                    workflow_id
                )
                logger.debug(
                    f"Scheduler status: {scheduler_status.get('scheduler_status')}"
                )
            except Exception as e:
                logger.warning(f"Failed to get scheduler status: {e}")
                # Continue with database status
            
            # Build step status list
            steps = []
            for step in workflow.get('steps', []):
                steps.append(StepStatus(
                    step_id=step.get('step_id', ''),
                    step_name=step.get('step_name', ''),
                    status=step.get('status', 'unknown'),
                    app=step.get('app', '')
                ))
            
            # Create status response
            status = WorkflowStatus(
                workflow_id=workflow['workflow_id'],
                workflow_name=workflow['workflow_name'],
                status=workflow.get('status', 'unknown'),
                created_at=workflow.get('created_at', datetime.utcnow()),
                updated_at=workflow.get('updated_at', datetime.utcnow()),
                steps=steps
            )
            
            logger.info(
                f"Workflow {workflow_id} status: {status.status}"
            )
            
            return status
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error retrieving workflow status: {e}")
            raise
    
    def get_full_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """Get complete workflow document.
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            Complete workflow dictionary
            
        Raises:
            ValueError: If workflow not found
        """
        logger.info(f"Retrieving full workflow {workflow_id}")
        
        workflow = self.state_manager.get_workflow(workflow_id)
        
        if not workflow:
            logger.error(f"Workflow {workflow_id} not found")
            raise ValueError(f"Workflow {workflow_id} not found")
        
        return workflow
    
    def update_workflow_status(
        self,
        workflow_id: str,
        status: str
    ) -> bool:
        """Update workflow status.
        
        Args:
            workflow_id: Workflow identifier
            status: New status value
            
        Returns:
            True if updated successfully
        """
        logger.info(f"Updating workflow {workflow_id} status to {status}")
        
        result = self.state_manager.update_workflow_status(
            workflow_id,
            status
        )
        
        if not result:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        return result
    
    def convert_cwl_workflow(self, cwl_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert CWL workflow to custom format.
        
        This method converts a CWL workflow to the custom workflow format,
        which can then be submitted using submit_workflow().
        
        Args:
            cwl_data: CWL workflow dictionary
            
        Returns:
            Custom workflow format dictionary
            
        Raises:
            ValueError: If CWL conversion fails
        """
        try:
            logger.info("Converting CWL workflow to custom format")
            
            # Parse and validate CWL
            cwl_workflow = self.cwl_parser.parse_cwl(cwl_data)
            self.cwl_parser.validate_cwl_workflow(cwl_workflow)
            
            # Convert to custom format
            custom_workflow = self.cwl_converter.convert(cwl_workflow)
            
            logger.info("CWL workflow conversion completed successfully")
            return custom_workflow
            
        except Exception as e:
            logger.error(f"CWL conversion failed: {e}")
            raise ValueError(f"Failed to convert CWL workflow: {e}")
    
    def submit_cwl_workflow(self, cwl_data: Dict[str, Any], auth_token: str = None) -> Dict[str, str]:
        """Submit a CWL workflow.
        
        This method converts a CWL workflow to custom format and submits it.
        It's a convenience method that combines convert_cwl_workflow() and submit_workflow().
        
        Args:
            cwl_data: CWL workflow dictionary
            auth_token: Optional authorization token for scheduler API calls
            
        Returns:
            Dictionary with workflow_id and status
            
        Raises:
            ValueError: If conversion or validation fails
            Exception: If submission fails
        """
        try:
            logger.info("Starting CWL workflow submission")
            
            # Convert CWL to custom format
            custom_workflow = self.convert_cwl_workflow(cwl_data)
            
            # Submit using existing workflow submission pipeline
            return self.submit_workflow(custom_workflow, auth_token=auth_token)
            
        except ValueError as e:
            logger.error(f"CWL workflow submission failed: {e}")
            raise
        except Exception as e:
            logger.error(f"CWL workflow submission failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        logger.info("Closing WorkflowManager")
        self.state_manager.close()

