"""CreateGroup step handler - executes group creation steps directly."""
import asyncio
import time
import random
from typing import Dict, Any, List, Optional
from datetime import datetime

from utils.logger import get_logger
from utils.workflow_logger import WorkflowLogger
from utils.variable_resolver import VariableResolver
from utils import metrics

# Import BV-BRC Groups Module
try:
    from bvbrc_groups_module import create_groups_from_job_results
    GROUPS_MODULE_AVAILABLE = True
except ImportError:
    GROUPS_MODULE_AVAILABLE = False


logger = get_logger(__name__)


class CreateGroupHandler:
    """Handles execution of CreateGroup workflow steps.
    
    CreateGroup steps are special workflow steps that create genome or feature
    groups from job results. They are executed directly by the workflow engine,
    not submitted to the scheduler.
    """
    
    def __init__(self, state_manager):
        """Initialize CreateGroup handler.
        
        Args:
            state_manager: MongoDB state manager for workflow state updates
        """
        self.state_manager = state_manager
        
        if not GROUPS_MODULE_AVAILABLE:
            logger.warning(
                "bvbrc_groups_module not available - CreateGroup steps will fail"
            )
    
    async def handle_create_group_step(
        self,
        workflow_id: str,
        step: Dict[str, Any],
        auth_token: str,
        workflow_logger,
        dag,
        mark_step_running,
        mark_step_completed,
        mark_step_failed
    ) -> None:
        """Handle CreateGroup step execution.
        
        CreateGroup steps are executed directly by the workflow engine,
        not submitted to the scheduler. They create genome or feature groups
        from job result paths using the bvbrc_groups_module.
        
        Args:
            workflow_id: Workflow identifier
            step: Step dictionary with CreateGroup configuration
            auth_token: Authentication token
            workflow_logger: Logger for workflow events
            dag: Workflow DAG
            mark_step_running: Callback to mark step as running
            mark_step_completed: Callback to mark step as completed
            mark_step_failed: Callback to mark step as failed
        """
        step_name = step.get('step_name')
        params = step.get('params', {})
        
        logger.info(f"Workflow {workflow_id}: Starting CreateGroup step '{step_name}'")
        
        # Check if groups module is available
        if not GROUPS_MODULE_AVAILABLE:
            error_msg = "bvbrc_groups_module is not installed or not available"
            logger.error(f"Workflow {workflow_id}: {error_msg}")
            await self._handle_failure(
                workflow_id, step, None, error_msg,
                workflow_logger, dag, mark_step_failed
            )
            return
        
        # Generate local step_id for this non-scheduler step
        step_id = self._generate_local_step_id(step_name)
        
        # Mark step as running
        logger.info(f"Workflow {workflow_id}: Marking CreateGroup step '{step_name}' as running")
        self.state_manager.update_step_by_name(
            workflow_id,
            step_name,
            {
                'step_id': step_id,
                'status': 'running',
                'submitted_at': datetime.utcnow(),
                'started_at': datetime.utcnow()
            }
        )
        
        # Update context
        mark_step_running(step_name)
        
        # Update DAG node status
        node_id = step_name
        if node_id in dag.nodes:
            dag.nodes[node_id]['status'] = 'running'
            dag.nodes[node_id]['step_id'] = step_id
        
        # Log start
        WorkflowLogger.log_workflow_event(
            workflow_logger,
            f"CreateGroup step '{step_name}' started",
            level="INFO"
        )
        
        # Resolve params (may contain variable references to previous steps)
        try:
            workflow_doc = self.state_manager.get_workflow(workflow_id)
            if workflow_doc:
                workflow_steps = workflow_doc.get('steps', [])
                params = VariableResolver.resolve_step_params_runtime(
                    params,
                    workflow_steps
                )
                logger.debug(f"Workflow {workflow_id}: Resolved CreateGroup params")
        except Exception as e:
            error_msg = f"Failed to resolve CreateGroup params: {str(e)}"
            logger.error(f"Workflow {workflow_id}: {error_msg}")
            await self._handle_failure(
                workflow_id, step, step_id, error_msg,
                workflow_logger, dag, mark_step_failed
            )
            return
        
        # Extract required parameters
        job_result_paths = params.get('job_result_paths', [])
        group_type = params.get('group_type')
        group_name = params.get('group_name')
        service_type = params.get('service_type')  # Optional
        output_group_path = params.get('output_group_path')  # Optional
        
        # Validate required parameters
        if not job_result_paths:
            error_msg = "CreateGroup step missing 'job_result_paths' parameter"
            logger.error(f"Workflow {workflow_id}: {error_msg}")
            await self._handle_failure(
                workflow_id, step, step_id, error_msg,
                workflow_logger, dag, mark_step_failed
            )
            return
        
        if not group_type:
            error_msg = "CreateGroup step missing 'group_type' parameter"
            logger.error(f"Workflow {workflow_id}: {error_msg}")
            await self._handle_failure(
                workflow_id, step, step_id, error_msg,
                workflow_logger, dag, mark_step_failed
            )
            return
        
        if not group_name:
            error_msg = "CreateGroup step missing 'group_name' parameter"
            logger.error(f"Workflow {workflow_id}: {error_msg}")
            await self._handle_failure(
                workflow_id, step, step_id, error_msg,
                workflow_logger, dag, mark_step_failed
            )
            return
        
        logger.info(
            f"Workflow {workflow_id}: CreateGroup params - "
            f"type={group_type}, name={group_name}, "
            f"{len(job_result_paths)} job path(s), service={service_type or 'auto'}"
        )
        
        # Log to workflow logger for user visibility
        WorkflowLogger.log_workflow_event(
            workflow_logger,
            f"CreateGroup: Creating {group_type} group '{group_name}' from {len(job_result_paths)} job result(s)",
            level="INFO"
        )
        
        # Execute group creation asynchronously to avoid blocking
        try:
            start_time = datetime.utcnow()
            
            # Run group creation in executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,  # Use default executor
                self._execute_group_creation,
                job_result_paths,
                group_type,
                group_name,
                service_type,
                output_group_path,
                auth_token
            )
            
            end_time = datetime.utcnow()
            elapsed = end_time - start_time
            elapsed_str = str(elapsed).split('.')[0]  # Remove microseconds
            
            # Check if group creation succeeded
            if not result.get('success'):
                error_msg = result.get('error', 'Unknown error from groups module')
                logger.error(
                    f"Workflow {workflow_id}: CreateGroup failed - {error_msg}"
                )
                
                # Log detailed statistics if available
                if 'statistics' in result:
                    stats = result['statistics']
                    logger.info(
                        f"Workflow {workflow_id}: CreateGroup statistics - "
                        f"total_ids={stats.get('total_ids_extracted', 0)}, "
                        f"unique_ids={stats.get('unique_ids', 0)}, "
                        f"valid_ids={stats.get('valid_ids', 0)}"
                    )
                
                await self._handle_failure(
                    workflow_id, step, step_id, error_msg,
                    workflow_logger, dag, mark_step_failed
                )
                return
            
            # Extract group path from result
            group_path = result.get('group_path')
            ids_count = result.get('ids_count', 0)
            jobs_processed = result.get('jobs_processed', 0)
            jobs_skipped = result.get('jobs_skipped', 0)
            
            logger.info(
                f"Workflow {workflow_id}: CreateGroup succeeded - "
                f"path={group_path}, ids={ids_count}, "
                f"jobs_processed={jobs_processed}, jobs_skipped={jobs_skipped}"
            )
            
            # Update step outputs with the created group path
            step_outputs = step.get('outputs', {})
            if 'group_path' in step_outputs:
                step_outputs['group_path'] = group_path
            else:
                step_outputs = {'group_path': group_path}
            
            # Mark step as completed
            self.state_manager.update_step_fields(
                workflow_id,
                step_id,
                {
                    'status': 'succeeded',
                    'completed_at': end_time,
                    'elapsed_time': elapsed_str,
                    'outputs': step_outputs
                }
            )
            
            # Update workflow metadata
            self.state_manager.add_to_completed_steps(workflow_id, step_id)
            
            # Update context
            mark_step_completed(step_name)
            
            # Update DAG node status
            if node_id in dag.nodes:
                dag.nodes[node_id]['status'] = 'succeeded'
                dag.nodes[node_id]['completed_at'] = end_time
                dag.nodes[node_id]['outputs'] = step_outputs
            
            # Log completion with details
            WorkflowLogger.log_step_completion(
                workflow_logger,
                step_name,
                elapsed_time=elapsed_str
            )
            
            # Log detailed results for user
            result_summary = f"CreateGroup results:\n"
            result_summary += f"  - Group created at: {group_path}\n"
            result_summary += f"  - Total IDs: {ids_count}\n"
            result_summary += f"  - Jobs processed: {jobs_processed}"
            if jobs_skipped > 0:
                result_summary += f"\n  - Jobs skipped: {jobs_skipped}"
            
            # Add statistics if available
            if 'statistics' in result:
                stats = result['statistics']
                result_summary += f"\n  - Statistics:"
                result_summary += f"\n    * IDs extracted: {stats.get('total_ids_extracted', 0)}"
                result_summary += f"\n    * Unique IDs: {stats.get('unique_ids', 0)}"
                result_summary += f"\n    * Valid IDs: {stats.get('valid_ids', 0)}"
                if stats.get('invalid_ids', 0) > 0:
                    result_summary += f"\n    * Invalid IDs: {stats.get('invalid_ids', 0)}"
            
            WorkflowLogger.log_workflow_event(
                workflow_logger,
                result_summary,
                level="INFO"
            )
            
            # Record metrics
            metrics.record_step_completed("CreateGroup", 'succeeded')
        
        except Exception as e:
            error_msg = f"CreateGroup execution failed: {str(e)}"
            logger.error(
                f"Workflow {workflow_id}: {error_msg}",
                exc_info=True
            )
            await self._handle_failure(
                workflow_id, step, step_id, error_msg,
                workflow_logger, dag, mark_step_failed
            )
    
    def _execute_group_creation(
        self,
        job_result_paths: List[str],
        group_type: str,
        group_name: str,
        service_type: Optional[str],
        output_group_path: Optional[str],
        auth_token: str
    ) -> Dict[str, Any]:
        """Execute group creation synchronously.
        
        This is run in a thread pool executor to avoid blocking the event loop.
        
        Args:
            job_result_paths: List of job result directory paths
            group_type: "genome" or "feature"
            group_name: Name for the group
            service_type: Optional service type filter
            output_group_path: Optional output path for group
            auth_token: Authentication token
            
        Returns:
            Result dictionary from create_groups_from_job_results
        """
        logger.info(f"Executing group creation: type={group_type}, name={group_name}")
        
        # Call the groups module
        result = create_groups_from_job_results(
            job_result_paths=job_result_paths,
            group_type=group_type,
            group_name=group_name,
            service_type=service_type,
            token=auth_token,
            output_group_path=output_group_path
        )
        
        logger.info(
            f"Group creation completed: success={result.get('success')}, "
            f"ids={result.get('ids_count', 0)}"
        )
        
        return result
    
    async def _handle_failure(
        self,
        workflow_id: str,
        step: Dict[str, Any],
        step_id: Optional[str],
        error_message: str,
        workflow_logger,
        dag,
        mark_step_failed
    ) -> None:
        """Handle CreateGroup step failure.
        
        Args:
            workflow_id: Workflow identifier
            step: Step dictionary
            step_id: Step identifier (may be None if failed before generation)
            error_message: Error description
            workflow_logger: Logger for workflow events
            dag: Workflow DAG
            mark_step_failed: Callback to mark step as failed
        """
        step_name = step.get('step_name')
        
        logger.error(
            f"Workflow {workflow_id}: CreateGroup step '{step_name}' failed: {error_message}"
        )
        
        # Update step in MongoDB (use step_id if available, otherwise step_name)
        if step_id:
            self.state_manager.update_step_fields(
                workflow_id,
                step_id,
                {
                    'status': 'failed',
                    'error_message': error_message,
                    'completed_at': datetime.utcnow()
                }
            )
        else:
            # If step_id not generated yet, update by name
            self.state_manager.update_step_by_name(
                workflow_id,
                step_name,
                {
                    'status': 'failed',
                    'error_message': error_message,
                    'completed_at': datetime.utcnow()
                }
            )
        
        # Update workflow metadata
        self.state_manager.increment_workflow_field(
            workflow_id,
            'execution_metadata.failed_steps',
            1
        )
        
        # Update context
        mark_step_failed(step_name)
        
        # Update DAG node status
        node_id = step_name
        if node_id in dag.nodes:
            dag.nodes[node_id]['status'] = 'failed'
            dag.nodes[node_id]['error_message'] = error_message
        
        # Log failure
        WorkflowLogger.log_step_failure(
            workflow_logger,
            step_name,
            error_message
        )
        
        # Record metrics
        metrics.record_step_completed("CreateGroup", 'failed')
    
    @staticmethod
    def _generate_local_step_id(step_name: str) -> str:
        """Generate a local step ID for non-scheduler steps.
        
        Used for CreateGroup and other special steps that don't get
        submitted to the scheduler.
        
        Args:
            step_name: Step name
            
        Returns:
            Generated step ID
        """
        timestamp = int(time.time() * 1000)
        random_part = random.randint(1000, 9999)
        return f"local_{step_name}_{timestamp}_{random_part}"

