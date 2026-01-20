"""Scheduler client for submitting jobs via JSON-RPC."""
import time
import random
from typing import Dict, Any, Optional, List

from utils.logger import get_logger
from utils.jsonrpc_client import JSONRPCClient
from models.workflow import WorkflowDefinition


logger = get_logger(__name__)


class SchedulerClient:
    """Client for interacting with the scheduler.
    
    Submits individual workflow steps as jobs to the scheduler using JSON-RPC.
    Each step's params are submitted to the appropriate app, and a task_id is returned.
    """
    
    def __init__(self, scheduler_url: str = None, timeout: int = 30, auth_token: Optional[str] = None):
        """Initialize scheduler client.
        
        Args:
            scheduler_url: URL of scheduler service (JSON-RPC endpoint)
            timeout: Request timeout in seconds
            auth_token: Optional authorization token for API requests
        """
        self.scheduler_url = scheduler_url
        self.timeout = timeout
        self.auth_token = auth_token  # Store auth token
        
        # Initialize JSON-RPC client if scheduler URL is provided
        self.jsonrpc_client: Optional[JSONRPCClient] = None
        if scheduler_url:
            self.jsonrpc_client = JSONRPCClient(
                base_url=scheduler_url,
                timeout=timeout,
                auth_token=auth_token
            )
            logger.info(
                f"Scheduler client initialized: url={scheduler_url}, timeout={timeout}"
            )
        else:
            logger.warning(
                "Scheduler client initialized without URL - will use placeholder mode"
            )
    
    def submit_workflow_to_scheduler(
        self,
        workflow: WorkflowDefinition,
        auth_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Submit workflow to scheduler and get assigned IDs.
        
        Process:
        1. Generate workflow ID (kept as current implementation)
        2. Convert workflow to dict and assign workflow_id
        3. For each step:
           - Generate step_id
           - Submit step's params to the appropriate app via JSON-RPC
           - Store returned task_id in the step
        4. Transform dependencies from step names to step IDs
        
        Args:
            workflow: Validated workflow definition
            auth_token: Optional authorization token for scheduler API calls
            
        Returns:
            Workflow dict with scheduler-assigned IDs and task_ids for each step
        """
        logger.info(
            f"Submitting workflow '{workflow.workflow_name}' to scheduler"
        )
        
        # Generate workflow ID (keep current implementation)
        workflow_id = self._generate_workflow_id()
        logger.info(f"Generated workflow_id: {workflow_id}")
        
        # Convert workflow to dict
        workflow_dict = workflow.model_dump()
        workflow_dict['workflow_id'] = workflow_id
        
        # Create JSON-RPC client for this submission (with auth_token if provided)
        jsonrpc_client = None
        if self.scheduler_url:
            jsonrpc_client = JSONRPCClient(
                base_url=self.scheduler_url,
                timeout=self.timeout,
                auth_token=auth_token
            )
        
        # Process each step: assign step_id and submit job to scheduler
        for i, step in enumerate(workflow_dict['steps']):
            # Generate step ID
            step_id = self._generate_step_id(i)
            step['step_id'] = step_id
            
            # Extract app and params from step
            app = step.get('app')
            params = step.get('params', {})
            
            if not app:
                raise ValueError(
                    f"Step '{step.get('step_name')}' missing 'app' field"
                )
            
            logger.info(
                f"Submitting step '{step.get('step_name')}' (step_id={step_id}) "
                f"to app '{app}'"
            )
            
            # Submit job to scheduler via JSON-RPC
            if jsonrpc_client:
                try:
                    task_id = jsonrpc_client.submit_job(
                        app=app,
                        params=params
                    )
                    # Store task_id in step
                    step['task_id'] = task_id
                    logger.info(
                        f"Step '{step.get('step_name')}' submitted successfully: "
                        f"task_id={task_id}"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to submit step '{step.get('step_name')}' to app '{app}': {e}"
                    )
                    raise
            else:
                # Fallback to placeholder mode if no scheduler URL
                logger.warning(
                    f"[PLACEHOLDER] No scheduler URL configured - "
                    f"generating mock task_id for step '{step.get('step_name')}'"
                )
                step['task_id'] = self._generate_task_id(step_id)
        
        # Transform depends_on from step names to step IDs
        self._transform_dependencies(workflow_dict['steps'])
        
        logger.info(
            f"Workflow '{workflow.workflow_name}' submitted successfully "
            f"with {len(workflow_dict['steps'])} steps"
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
    
    def submit_job(
        self,
        app: str,
        params: Dict[str, Any],
        auth_token: Optional[str] = None
    ) -> str:
        """Submit a single job to the scheduler.
        
        This is used by the executor to submit individual workflow steps.
        
        Args:
            app: Application name (e.g., "GenomeAssembly2", "GenomeAnnotation")
            params: Job parameters
            auth_token: Authorization token for this submission
            
        Returns:
            Task ID assigned by scheduler (becomes step_id)
            
        Raises:
            Exception: If submission fails
        """
        logger.info(f"Submitting job to app '{app}' with auth token")
        
        # Create a temporary JSON-RPC client with the provided auth token
        if auth_token:
            temp_client = JSONRPCClient(
                base_url=self.scheduler_url,
                timeout=self.timeout,
                auth_token=auth_token
            )
            task_id = temp_client.submit_job(app=app, params=params)
        elif self.jsonrpc_client:
            task_id = self.jsonrpc_client.submit_job(app=app, params=params)
        else:
            raise ValueError("No scheduler URL configured")
        
        logger.info(f"Job submitted successfully to '{app}': task_id={task_id}")
        return task_id
    
    def query_task_status(
        self,
        task_ids: List[str],
        auth_token: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Query scheduler for task status.
        
        Makes JSON-RPC call to AppService.query_tasks to check status of jobs.
        
        The JSON-RPC call format is:
        {
          "jsonrpc": "2.0",
          "method": "AppService.query_tasks",
          "params": [["task_id1", "task_id2", ...]],
          "id": request_id
        }
        
        Response format:
        {
          "jsonrpc": "2.0",
          "result": [{
            "task_id1": {
              "status": "completed|running|failed",
              "completed_time": "2025-12-11 17:47:38",
              "elapsed_time": "00:04:33",
              "id": "task_id1",
              ...
            },
            "task_id2": {...}
          }],
          "id": request_id
        }
        
        Args:
            task_ids: List of task IDs to query
            auth_token: Optional authorization token (overrides client's token)
            
        Returns:
            Dictionary mapping task_id to status information
            
        Raises:
            requests.exceptions.RequestException: If HTTP request fails
            ValueError: If JSON-RPC response contains an error
        """
        if not task_ids:
            logger.warning("query_task_status called with empty task_ids list")
            return {}
        
        method = "AppService.query_tasks"
        
        # Note: params is nested array [[task_ids]]
        rpc_params = [task_ids]
        
        # Use provided auth_token or fall back to client's token
        token = auth_token or self.auth_token
        
        logger.debug(
            f"Querying task status for {len(task_ids)} tasks:\n"
            f"  Task IDs: {task_ids}\n"
            f"  Auth token present: {bool(token)}"
        )
        
        try:
            # Create temporary client with this specific auth token if needed
            if auth_token and auth_token != self.auth_token:
                temp_client = JSONRPCClient(
                    base_url=self.scheduler_url,
                    timeout=self.timeout,
                    auth_token=auth_token
                )
                result = temp_client.call(method, rpc_params)
            elif self.jsonrpc_client:
                result = self.jsonrpc_client.call(method, rpc_params)
            else:
                logger.warning("No JSON-RPC client available for query_task_status")
                return {}
            
            # Parse response: result is a list with one dict containing task statuses
            if isinstance(result, list) and len(result) > 0:
                status_dict = result[0]
                
                if not isinstance(status_dict, dict):
                    logger.error(f"Unexpected result format: {result}")
                    raise ValueError(f"Expected dict in result list, got {type(status_dict)}")
                
                logger.debug(f"Received status for {len(status_dict)} tasks")
                return status_dict
            
            elif isinstance(result, dict):
                # Sometimes result might be the dict directly
                logger.debug(f"Received status dict directly for {len(result)} tasks")
                return result
            
            else:
                logger.warning(f"Unexpected result format from query_tasks: {result}")
                return {}
        
        except Exception as e:
            logger.error(f"Error querying task status: {e}")
            raise
    
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
        """Generate step ID.
        
        Args:
            index: Step index in workflow
            
        Returns:
            Step ID string
        """
        timestamp = int(time.time() * 1000)
        random_part = random.randint(1000, 9999)
        return f"step_{timestamp}_{index}_{random_part}"
    
    @staticmethod
    def _generate_task_id(step_id: str) -> str:
        """Generate mock task ID for placeholder mode.
        
        Args:
            step_id: Step ID to base task ID on
            
        Returns:
            Task ID string
        """
        timestamp = int(time.time() * 1000)
        random_part = random.randint(1000, 9999)
        return f"task_{timestamp}_{random_part}"
    
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

