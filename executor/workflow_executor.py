"""Workflow executor - orchestrates DAG-based workflow execution."""
import asyncio
import json
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from core.state_manager import StateManager
from scheduler.client import SchedulerClient
from executor.workflow_context import WorkflowExecutionContext
from executor.create_group_handler import CreateGroupHandler
from utils.logger import get_logger
from utils.workflow_logger import WorkflowLogger
from utils.variable_resolver import VariableResolver
from utils import metrics


logger = get_logger(__name__)

HOMOLOGY_PRECOMPUTED_DATABASES = {"bacteria-archaea", "viral-reference"}


class WorkflowExecutor:
    """Orchestrates DAG-based workflow execution.
    
    Responsibilities:
    - Load active workflows from MongoDB
    - Build and maintain execution contexts
    - Poll scheduler for job status
    - Submit ready steps respecting parallelism limits
    - Update workflow state in MongoDB
    - Write to per-workflow log files
    """
    
    def __init__(
        self,
        state_manager: StateManager,
        scheduler_client: SchedulerClient,
        config: Any
    ):
        """Initialize workflow executor.
        
        Args:
            state_manager: MongoDB state manager
            scheduler_client: Scheduler client for job submission/queries
            config: Configuration object
        """
        self.state_manager = state_manager
        self.scheduler_client = scheduler_client
        self.config = config
        
        # Active workflow contexts (in-memory)
        self.active_workflows: Dict[str, WorkflowExecutionContext] = {}
        
        # APScheduler instance
        self.scheduler = AsyncIOScheduler()
        
        # CreateGroup handler
        self.create_group_handler = CreateGroupHandler(state_manager)
        
        # Configuration
        self.polling_interval = config.executor.get('polling_interval_seconds', 10)
        self.enable_auto_resume = config.executor.get('enable_auto_resume', True)
        
        # Shutdown flag
        self._shutdown = False
        
        logger.info(
            f"WorkflowExecutor initialized: "
            f"polling_interval={self.polling_interval}s, "
            f"auto_resume={self.enable_auto_resume}"
        )
    
    async def start(self) -> None:
        """Start the workflow executor."""
        logger.info("Starting workflow executor...")
        
        # Resume workflows if enabled
        if self.enable_auto_resume:
            await self.resume_active_workflows()
        
        # Schedule the polling job
        self.scheduler.add_job(
            self.poll_and_execute,
            trigger=IntervalTrigger(seconds=self.polling_interval),
            id='workflow_poller',
            replace_existing=True,
            max_instances=1,  # Prevent overlapping executions
            misfire_grace_time=30  # Grace period for missed jobs
        )
        
        self.scheduler.start()
        
        logger.info(
            f"Workflow executor started (polling every {self.polling_interval}s)"
        )
    
    async def stop(self) -> None:
        """Stop the executor gracefully."""
        logger.info("Stopping workflow executor...")
        self._shutdown = True
        
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
        
        # Close all workflow loggers
        for workflow_id in list(self.active_workflows.keys()):
            WorkflowLogger.close_logger(workflow_id)
        
        logger.info("Workflow executor stopped")
    
    async def resume_active_workflows(self) -> None:
        """Resume workflows that were running when executor was stopped."""
        try:
            logger.info("Resuming active workflows from database...")
            
            # Get workflows in running or queued state
            workflows = self.state_manager.get_active_workflows()
            
            resumed_count = 0
            for workflow_doc in workflows:
                try:
                    workflow_id = workflow_doc['workflow_id']
                    logger.info(f"Resuming workflow {workflow_id}")
                    
                    # Build execution context
                    ctx = WorkflowExecutionContext.build_from_workflow_document(
                        workflow_doc,
                        log_dir=self.config.logging.get('workflow_log_dir', 'logs/workflows')
                    )
                    
                    # Add to active workflows
                    self.active_workflows[workflow_id] = ctx
                    
                    # Log resumption
                    WorkflowLogger.log_workflow_event(
                        ctx.workflow_logger,
                        "Workflow execution resumed after executor restart",
                        level="INFO"
                    )
                    
                    resumed_count += 1
                
                except Exception as e:
                    workflow_id = workflow_doc.get('workflow_id', 'unknown')
                    logger.error(
                        f"Failed to resume workflow {workflow_id}: {e}",
                        exc_info=True
                    )
            
            logger.info(f"Resumed {resumed_count} workflows")
            
        except Exception as e:
            logger.error(f"Error resuming workflows: {e}", exc_info=True)
    
    async def poll_and_execute(self) -> None:
        """Main polling and execution loop.
        
        Called every polling_interval seconds by APScheduler.
        """
        if self._shutdown:
            return
        
        poll_start_time = time.time()
        
        try:
            logger.debug("=== Poll cycle started ===")
            
            # 1. Load new pending workflows
            await self.load_pending_workflows()
            
            # 2. Process each active workflow
            for workflow_id in list(self.active_workflows.keys()):
                try:
                    ctx = self.active_workflows[workflow_id]
                    await self.process_workflow(ctx)
                except Exception as e:
                    logger.error(
                        f"Error processing workflow {workflow_id}: {e}",
                        exc_info=True
                    )
                    await self.handle_workflow_error(workflow_id, str(e))
            
            # 3. Update metrics
            metrics.update_active_workflows(len(self.active_workflows))
            
            # Record poll cycle
            poll_duration = time.time() - poll_start_time
            metrics.record_poll_cycle()
            metrics.record_poll_duration(poll_duration)
            
            logger.debug(
                f"=== Poll cycle completed in {poll_duration:.2f}s ==="
            )
        
        except Exception as e:
            logger.error(f"Error in poll cycle: {e}", exc_info=True)
            metrics.record_executor_error('poll_cycle_error')
    
    async def load_pending_workflows(self) -> None:
        """Load workflows with status='pending' and create contexts."""
        try:
            pending_workflows = self.state_manager.get_workflows_by_status('pending')
            
            for workflow_doc in pending_workflows:
                workflow_id = workflow_doc['workflow_id']
                
                # Skip if already loaded
                if workflow_id in self.active_workflows:
                    continue
                
                try:
                    logger.info(f"Loading pending workflow: {workflow_id}")
                    
                    # Build execution context
                    ctx = WorkflowExecutionContext.build_from_workflow_document(
                        workflow_doc,
                        log_dir=self.config.logging.get('workflow_log_dir', 'logs/workflows')
                    )
                    
                    # Add to active workflows
                    self.active_workflows[workflow_id] = ctx
                    
                    # Update status to queued
                    self.state_manager.update_workflow_status(workflow_id, 'queued')
                    ctx.update_status('queued')
                    
                    # Log workflow start
                    WorkflowLogger.log_workflow_start(
                        ctx.workflow_logger,
                        ctx.workflow_name,
                        ctx.total_steps
                    )
                    
                    logger.info(f"Workflow {workflow_id} queued for execution")
                
                except Exception as e:
                    logger.error(
                        f"Failed to load workflow {workflow_id}: {e}",
                        exc_info=True
                    )
        
        except Exception as e:
            logger.error(f"Error loading pending workflows: {e}", exc_info=True)
    
    async def process_workflow(self, ctx: WorkflowExecutionContext) -> None:
        """Process a single workflow.
        
        Args:
            ctx: Workflow execution context
        """
        workflow_id = ctx.workflow_id
        
        # Update poll time
        ctx.last_poll_time = datetime.utcnow()
        
        # Check if workflow was cancelled
        if ctx.status == 'cancelled':
            await self.handle_workflow_cancellation(ctx)
            return
        
        # Check if workflow is complete
        if ctx.is_complete():
            if ctx.has_succeeded():
                await self.handle_workflow_completion(ctx, 'succeeded')
            elif ctx.has_failed():
                await self.handle_workflow_completion(ctx, 'failed')
            return
        
        # Check for failed steps
        if ctx.has_failed() and ctx.status != 'failed':
            await self.handle_workflow_completion(ctx, 'failed')
            return
        
        # Transition to running if still queued
        if ctx.status == 'queued':
            self.state_manager.update_workflow_fields(
                workflow_id,
                {
                    'status': 'running',
                    'started_at': datetime.utcnow()
                }
            )
            ctx.update_status('running')
            WorkflowLogger.log_workflow_event(
                ctx.workflow_logger,
                "Workflow execution started",
                level="INFO"
            )
        
        # Poll scheduler for running steps
        running_steps = ctx.get_running_steps_list()
        if running_steps:
            await self.check_running_steps(ctx, running_steps)
        
        # Submit ready steps if capacity available
        if ctx.can_submit_more_steps():
            await self.submit_ready_steps(ctx)
    
    async def check_running_steps(
        self,
        ctx: WorkflowExecutionContext,
        running_steps: List[Dict[str, Any]]
    ) -> None:
        """Check status of running steps with scheduler.
        
        Args:
            ctx: Workflow execution context
            running_steps: List of running step dictionaries
        """
        workflow_id = ctx.workflow_id
        
        # Extract task_ids
        task_ids = [step.get('task_id') for step in running_steps if step.get('task_id')]
        
        if not task_ids:
            logger.warning(f"Workflow {workflow_id}: No task_ids found for running steps")
            return
        
        try:
            query_start = time.time()
            
            # Query scheduler
            status_results = self.scheduler_client.query_task_status(
                task_ids,
                auth_token=ctx.auth_token
            )
            
            query_duration = time.time() - query_start
            metrics.record_scheduler_query_duration(query_duration)
            
            logger.debug(
                f"Workflow {workflow_id}: Queried status for {len(task_ids)} tasks "
                f"in {query_duration:.2f}s"
            )
            
            # Process each running step
            for step in running_steps:
                task_id = step.get('task_id')
                if not task_id or task_id not in status_results:
                    continue
                
                task_status_info = status_results[task_id]
                scheduler_status = task_status_info.get('status')
                
                if scheduler_status == 'completed':
                    await self.handle_step_completion(ctx, step, task_status_info)
                elif scheduler_status == 'failed':
                    await self.handle_step_failure(ctx, step, task_status_info)
                # If 'running', do nothing (continue polling)
        
        except Exception as e:
            logger.error(
                f"Workflow {workflow_id}: Failed to query task status: {e}",
                exc_info=True
            )
            metrics.record_scheduler_query_error()
    
    async def submit_ready_steps(self, ctx: WorkflowExecutionContext) -> None:
        """Submit steps that are ready to run.
        
        Args:
            ctx: Workflow execution context
        """
        workflow_id = ctx.workflow_id
        
        # Get capacity
        capacity = ctx.get_capacity()
        if capacity <= 0:
            return
        
        # Get ready steps
        ready_steps = ctx.get_ready_steps()
        if not ready_steps:
            return
        
        logger.info(
            f"Workflow {workflow_id}: {len(ready_steps)} steps ready, "
            f"capacity={capacity}"
        )
        
        # Submit up to capacity
        steps_to_submit = ready_steps[:capacity]
        
        for step in steps_to_submit:
            try:
                await self.submit_step(ctx, step)
            except Exception as e:
                step_name = step.get('step_name', 'unknown')
                logger.error(
                    f"Workflow {workflow_id}: Failed to submit step {step_name}: {e}",
                    exc_info=True
                )
                # Mark step as failed
                await self.handle_step_submission_failure(ctx, step, str(e))
    
    async def submit_step(
        self,
        ctx: WorkflowExecutionContext,
        step: Dict[str, Any]
    ) -> None:
        """Submit a single step to the scheduler.
        
        Special handling for CreateGroup steps - these are executed directly
        by the workflow engine, not submitted to the scheduler.
        
        Args:
            ctx: Workflow execution context
            step: Step dictionary
        """
        workflow_id = ctx.workflow_id
        step_name = step.get('step_name')
        app = step.get('app')
        params = step.get('params', {})
        
        # Check if this is a CreateGroup step
        if app == "CreateGroup":
            logger.info(f"Workflow {workflow_id}: Executing CreateGroup step '{step_name}'")
            await self.create_group_handler.handle_create_group_step(
                workflow_id=workflow_id,
                step=step,
                auth_token=ctx.auth_token,
                workflow_logger=ctx.workflow_logger,
                dag=ctx.dag,
                mark_step_running=ctx.mark_step_running,
                mark_step_completed=ctx.mark_step_completed,
                mark_step_failed=ctx.mark_step_failed
            )
            return
        
        logger.info(f"Workflow {workflow_id}: Submitting step '{step_name}' to app '{app}'")
        
        # Log the step details before resolution
        logger.info(
            f"Workflow {workflow_id}: Step '{step_name}' details before runtime resolution:\n"
            f"  App: {app}\n"
            f"  Params: {json.dumps(params, indent=2)}"
        )
        
        try:
            # IMPORTANT: Resolve step output references in params before submission
            # Get current workflow state from MongoDB to get latest step outputs
            workflow_doc = self.state_manager.get_workflow(workflow_id)
            if workflow_doc:
                workflow_steps = workflow_doc.get('steps', [])
                params = VariableResolver.resolve_step_params_runtime(
                    params,
                    workflow_steps
                )
                logger.info(
                    f"Workflow {workflow_id}: Resolved params for step '{step_name}' after runtime resolution:\n"
                    f"  Params: {json.dumps(params, indent=2)}"
                )
            
            # Defensive check: Validate critical parameters before submission
            # This catches cases where validation might have been bypassed or parameters changed
            app_lower = app.lower() if app else ""
            if "homology" in app_lower or app_lower == "blast":
                db_source = params.get("db_source")
                if db_source == "precomputed_database":
                    db_precomputed = params.get("db_precomputed_database")
                    if not db_precomputed or (isinstance(db_precomputed, str) and db_precomputed.strip() == ""):
                        error_msg = (
                            f"Workflow {workflow_id}: Step '{step_name}' has db_source='precomputed_database' "
                            f"but db_precomputed_database is missing or empty. "
                            f"This should have been caught during validation."
                        )
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                    candidate = db_precomputed.strip().lower() if isinstance(db_precomputed, str) else db_precomputed
                    if candidate not in HOMOLOGY_PRECOMPUTED_DATABASES:
                        error_msg = (
                            f"Workflow {workflow_id}: Step '{step_name}' has invalid "
                            f"db_precomputed_database={db_precomputed!r}. Allowed values: "
                            f"{sorted(HOMOLOGY_PRECOMPUTED_DATABASES)}."
                        )
                        logger.error(error_msg)
                        raise ValueError(error_msg)
            
            # Log the full job spec being sent to scheduler
            logger.info(
                f"Workflow {workflow_id}: Full job spec being sent to scheduler for step '{step_name}':\n"
                f"  App: {app}\n"
                f"  Params: {json.dumps(params, indent=2)}\n"
                f"  Auth token present: {bool(ctx.auth_token)}"
            )
            
            # Submit to scheduler
            task_id = self.scheduler_client.submit_job(
                app=app,
                params=params,
                auth_token=ctx.auth_token
            )
            
            # Task ID becomes step ID (per plan)
            step_id = task_id
            
            logger.info(
                f"Workflow {workflow_id}: Step '{step_name}' submitted successfully "
                f"(step_id={step_id}, task_id={task_id})"
            )
            
            # Update step in MongoDB
            self.state_manager.update_step_by_name(
                workflow_id,
                step_name,
                {
                    'step_id': step_id,
                    'task_id': task_id,
                    'status': 'running',
                    'submitted_at': datetime.utcnow()
                }
            )
            
            # Update workflow metadata
            self.state_manager.add_to_running_steps(workflow_id, step_id)
            
            # Update context (track by step_name for consistency with DAG)
            ctx.mark_step_running(step_name)
            
            # **IMPORTANT**: Update the DAG node status to prevent re-submission
            # in the same poll cycle
            node_id = step_name  # Nodes are indexed by step_name
            if node_id in ctx.dag.nodes:
                ctx.dag.nodes[node_id]['status'] = 'running'
                ctx.dag.nodes[node_id]['step_id'] = step_id
                ctx.dag.nodes[node_id]['task_id'] = task_id
            
            # Log submission
            WorkflowLogger.log_step_submission(
                ctx.workflow_logger,
                step_name,
                app,
                task_id
            )
            
            # Record metrics
            metrics.record_step_submitted(app)
        
        except Exception as e:
            logger.error(
                f"Workflow {workflow_id}: Step '{step_name}' submission failed: {e}",
                exc_info=True
            )
            metrics.record_scheduler_submit_error(app)
            raise
    
    async def handle_step_completion(
        self,
        ctx: WorkflowExecutionContext,
        step: Dict[str, Any],
        task_status_info: Dict[str, Any]
    ) -> None:
        """Handle successful step completion.
        
        Args:
            ctx: Workflow execution context
            step: Step dictionary
            task_status_info: Status info from scheduler
        """
        workflow_id = ctx.workflow_id
        step_id = step.get('step_id')
        step_name = step.get('step_name')
        app = step.get('app')
        elapsed_time = task_status_info.get('elapsed_time')
        
        logger.info(
            f"Workflow {workflow_id}: Step '{step_name}' completed "
            f"(elapsed_time={elapsed_time})"
        )
        
        # Update step in MongoDB
        self.state_manager.update_step_fields(
            workflow_id,
            step_id,
            {
                'status': 'succeeded',
                'completed_at': datetime.utcnow(),
                'elapsed_time': elapsed_time
            }
        )
        
        # Update workflow metadata
        self.state_manager.remove_from_running_steps(workflow_id, step_id)
        self.state_manager.add_to_completed_steps(workflow_id, step_id)
        
        # Update context (track by step_name for consistency with DAG)
        ctx.mark_step_completed(step_name)
        
        # Update DAG node status (nodes are always indexed by step_name)
        node_id = step_name
        if node_id in ctx.dag.nodes:
            ctx.dag.nodes[node_id]['status'] = 'succeeded'
            ctx.dag.nodes[node_id]['completed_at'] = datetime.utcnow()
        
        # Log completion
        WorkflowLogger.log_step_completion(
            ctx.workflow_logger,
            step_name,
            elapsed_time=elapsed_time
        )
        
        # Record metrics
        metrics.record_step_completed(app, 'succeeded')
        
        # Parse elapsed time and record duration
        if elapsed_time:
            try:
                duration_seconds = self._parse_elapsed_time(elapsed_time)
                metrics.record_step_duration(app, duration_seconds)
            except Exception:
                pass
    
    async def handle_step_failure(
        self,
        ctx: WorkflowExecutionContext,
        step: Dict[str, Any],
        task_status_info: Dict[str, Any]
    ) -> None:
        """Handle step failure.
        
        Args:
            ctx: Workflow execution context
            step: Step dictionary
            task_status_info: Status info from scheduler
        """
        workflow_id = ctx.workflow_id
        step_id = step.get('step_id')
        step_name = step.get('step_name')
        app = step.get('app')
        error_message = task_status_info.get('error', 'Unknown error from scheduler')
        
        logger.error(
            f"Workflow {workflow_id}: Step '{step_name}' FAILED | error={error_message}"
        )
        
        # Update step in MongoDB
        self.state_manager.update_step_fields(
            workflow_id,
            step_id,
            {
                'status': 'failed',
                'completed_at': datetime.utcnow(),
                'error_message': error_message
            }
        )
        
        # Update workflow metadata
        self.state_manager.remove_from_running_steps(workflow_id, step_id)
        self.state_manager.increment_workflow_field(
            workflow_id,
            'execution_metadata.failed_steps',
            1
        )
        
        # Update context (track by step_name for consistency with DAG)
        ctx.mark_step_failed(step_name)
        
        # Update DAG node status (nodes are always indexed by step_name)
        node_id = step_name
        if node_id in ctx.dag.nodes:
            ctx.dag.nodes[node_id]['status'] = 'failed'
            ctx.dag.nodes[node_id]['error_message'] = error_message
        
        # Log failure
        WorkflowLogger.log_step_failure(
            ctx.workflow_logger,
            step_name,
            error_message
        )
        
        # Record metrics
        metrics.record_step_completed(app, 'failed')
    
    async def handle_step_submission_failure(
        self,
        ctx: WorkflowExecutionContext,
        step: Dict[str, Any],
        error_message: str
    ) -> None:
        """Handle failure to submit step.
        
        Args:
            ctx: Workflow execution context
            step: Step dictionary
            error_message: Error description
        """
        workflow_id = ctx.workflow_id
        step_name = step.get('step_name')
        
        logger.error(
            f"Workflow {workflow_id}: Step '{step_name}' submission failed: {error_message}"
        )
        
        # Update step in MongoDB
        self.state_manager.update_step_by_name(
            workflow_id,
            step_name,
            {
                'status': 'failed',
                'error_message': f"Submission failed: {error_message}",
                'completed_at': datetime.utcnow()
            }
        )
        
        # Update workflow metadata
        self.state_manager.increment_workflow_field(
            workflow_id,
            'execution_metadata.failed_steps',
            1
        )
        
        # Update context (mark as failed by step_name)
        ctx.failed_steps.add(step_name)
        
        # Update DAG node status (nodes are always indexed by step_name)
        node_id = step_name
        if node_id in ctx.dag.nodes:
            ctx.dag.nodes[node_id]['status'] = 'failed'
            ctx.dag.nodes[node_id]['error_message'] = f"Submission failed: {error_message}"
        
        # Log failure
        WorkflowLogger.log_step_failure(
            ctx.workflow_logger,
            step_name,
            f"Submission failed: {error_message}"
        )
    
    async def handle_workflow_completion(
        self,
        ctx: WorkflowExecutionContext,
        final_status: str
    ) -> None:
        """Handle workflow completion.
        
        Args:
            ctx: Workflow execution context
            final_status: Final status (succeeded/failed)
        """
        workflow_id = ctx.workflow_id
        workflow_name = ctx.workflow_name
        
        logger.info(f"Workflow {workflow_id} completed with status: {final_status}")
        
        # Calculate duration
        workflow_doc = self.state_manager.get_workflow(workflow_id)
        started_at = workflow_doc.get('started_at')
        duration_str = None
        
        if started_at:
            duration = datetime.utcnow() - started_at
            duration_seconds = duration.total_seconds()
            duration_str = str(duration)
            
            # Record metrics
            metrics.record_workflow_duration(duration_seconds)
        
        # Update workflow in MongoDB
        self.state_manager.update_workflow_fields(
            workflow_id,
            {
                'status': final_status,
                'completed_at': datetime.utcnow()
            }
        )
        
        # Log completion
        WorkflowLogger.log_workflow_completion(
            ctx.workflow_logger,
            workflow_name,
            final_status,
            duration=duration_str
        )
        
        # Record metrics
        metrics.record_workflow_completed(final_status)
        
        # Close logger
        WorkflowLogger.close_logger(workflow_id)
        
        # Remove from active workflows
        if workflow_id in self.active_workflows:
            del self.active_workflows[workflow_id]
        
        logger.info(f"Workflow {workflow_id} removed from active workflows")
    
    async def handle_workflow_cancellation(self, ctx: WorkflowExecutionContext) -> None:
        """Handle workflow cancellation.
        
        Args:
            ctx: Workflow execution context
        """
        workflow_id = ctx.workflow_id
        
        logger.info(f"Workflow {workflow_id} was cancelled")
        
        # Log cancellation
        WorkflowLogger.log_workflow_event(
            ctx.workflow_logger,
            "Workflow cancelled by user",
            level="WARNING"
        )
        
        # Record metrics
        metrics.record_workflow_completed('cancelled')
        
        # Close logger
        WorkflowLogger.close_logger(workflow_id)
        
        # Remove from active workflows
        if workflow_id in self.active_workflows:
            del self.active_workflows[workflow_id]
        
        logger.info(f"Workflow {workflow_id} removed from active workflows")
    
    async def handle_workflow_error(self, workflow_id: str, error_message: str) -> None:
        """Handle unexpected workflow error.
        
        Args:
            workflow_id: Workflow identifier
            error_message: Error description
        """
        logger.error(f"Workflow {workflow_id} encountered error: {error_message}")
        
        try:
            # Update workflow status
            self.state_manager.update_workflow_fields(
                workflow_id,
                {
                    'status': 'failed',
                    'error_message': f"Executor error: {error_message}",
                    'completed_at': datetime.utcnow()
                }
            )
            
            # Log error if logger exists
            if workflow_id in self.active_workflows:
                ctx = self.active_workflows[workflow_id]
                WorkflowLogger.log_workflow_event(
                    ctx.workflow_logger,
                    f"Workflow failed due to executor error: {error_message}",
                    level="ERROR"
                )
                WorkflowLogger.close_logger(workflow_id)
                del self.active_workflows[workflow_id]
            
            # Record metrics
            metrics.record_workflow_completed('failed')
            metrics.record_executor_error('workflow_processing_error')
        
        except Exception as e:
            logger.error(f"Error handling workflow error: {e}", exc_info=True)
    
    @staticmethod
    def _parse_elapsed_time(elapsed_str: str) -> float:
        """Parse elapsed time string to seconds.
        
        Args:
            elapsed_str: Elapsed time string (e.g., "00:04:33")
            
        Returns:
            Duration in seconds
        """
        parts = elapsed_str.split(':')
        if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
        return 0.0

