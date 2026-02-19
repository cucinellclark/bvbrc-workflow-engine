"""Workflow manager - orchestrates workflow submission and status queries."""
import json
from typing import Dict, Any
from datetime import datetime

from core.validator import WorkflowValidator
from core.state_manager import StateManager
from scheduler.client import SchedulerClient
from models.workflow import WorkflowStatus, StepStatus
from utils.logger import get_logger
from utils.variable_resolver import VariableResolver
from utils.workflow_cleaner import clean_empty_optional_lists
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

    def register_workflow(self, workflow_json: Dict[str, Any], auth_token: str = None) -> Dict[str, Any]:
        """Register and persist a validated workflow without submitting it.

        Process:
        1. Resolve variable placeholders
        2. Validate workflow JSON
        3. Generate workflow_id
        4. Save to MongoDB with status='planned'
        5. Return workflow identity and summary metadata

        Args:
            workflow_json: Raw workflow JSON dictionary
            auth_token: Optional authorization token for workspace-aware validation

        Returns:
            Dictionary with workflow_id, status, workflow_name, and step_count
        """
        try:
            logger.info("Starting workflow registration")

            logger.info(
                f"Raw workflow JSON received for registration:\n{json.dumps(workflow_json, indent=2)}"
            )

            # Step 1: Clean up empty optional lists before processing
            logger.info("Cleaning up empty optional lists in workflow")
            cleaned_workflow = clean_empty_optional_lists(workflow_json)

            # Step 2: Resolve variable placeholders
            logger.info("Resolving variable placeholders for registration")
            resolved_workflow = VariableResolver.resolve_workflow_variables(cleaned_workflow)

            # Step 3: Validate workflow
            logger.info("Validating workflow for registration")
            validated_workflow = self.validator.validate_workflow_input(
                resolved_workflow,
                auth_token=auth_token
            )

            # Generate workflow_id only if not already present
            workflow_dict = validated_workflow.model_dump()
            if 'workflow_id' in resolved_workflow and resolved_workflow['workflow_id']:
                workflow_id = resolved_workflow['workflow_id']
                logger.info(f"Using existing workflow_id for registration: {workflow_id}")
            else:
                workflow_id = self._generate_workflow_id()
                logger.info(f"Generated new workflow_id for registration: {workflow_id}")
            
            workflow_dict['workflow_id'] = workflow_id
            workflow_dict['status'] = 'planned'
            workflow_dict['created_at'] = datetime.utcnow()
            workflow_dict['updated_at'] = datetime.utcnow()

            # Planned workflows should not have execution state initialized yet.
            workflow_dict.pop('execution_metadata', None)
            workflow_dict.pop('log_file_path', None)
            workflow_dict.pop('started_at', None)
            workflow_dict.pop('completed_at', None)

            # Store auth token (plaintext for now - TODO: encrypt)
            if auth_token:
                workflow_dict['auth_token'] = auth_token

            # Keep steps explicitly planned so UI can render intent before execution.
            for step in workflow_dict.get('steps', []):
                if 'status' not in step:
                    step['status'] = 'planned'

            logger.info(f"Saving registered workflow {workflow_id} to database")
            self.state_manager.save_workflow(workflow_dict)

            step_count = len(workflow_dict.get('steps', []))
            logger.info(
                f"Workflow '{validated_workflow.workflow_name}' registered successfully "
                f"with ID: {workflow_id} (status=planned)"
            )

            return {
                'workflow_id': workflow_id,
                'status': 'planned',
                'workflow_name': validated_workflow.workflow_name,
                'step_count': step_count
            }

        except ValueError as e:
            logger.error(f"Workflow registration validation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Workflow registration failed: {e}")
            raise

    def plan_workflow(self, workflow_json: Dict[str, Any], auth_token: str = None) -> Dict[str, Any]:
        """Persist a workflow plan without validation.

        Planning intentionally stores the user/LLM-produced job spec as-is (after
        lightweight cleanup/variable resolution) so validation can be a separate,
        explicit stage.
        """
        try:
            logger.info("Starting workflow planning (no validation)")
            if not isinstance(workflow_json, dict):
                raise ValueError("plan_workflow requires a JSON object")

            cleaned_workflow = clean_empty_optional_lists(workflow_json)
            planned_workflow = VariableResolver.resolve_workflow_variables(cleaned_workflow)

            workflow_id = self._generate_workflow_id()
            workflow_name = planned_workflow.get("workflow_name") or "Planned Workflow"

            workflow_dict = json.loads(json.dumps(planned_workflow))
            workflow_dict["workflow_id"] = workflow_id
            workflow_dict["status"] = "planned"
            workflow_dict["created_at"] = datetime.utcnow()
            workflow_dict["updated_at"] = datetime.utcnow()

            # Planned workflows should not have execution state initialized yet.
            workflow_dict.pop("execution_metadata", None)
            workflow_dict.pop("log_file_path", None)
            workflow_dict.pop("started_at", None)
            workflow_dict.pop("completed_at", None)

            if auth_token:
                workflow_dict["auth_token"] = auth_token

            steps = workflow_dict.get("steps", [])
            if not isinstance(steps, list):
                raise ValueError("Workflow plan must contain a 'steps' array")

            for step in steps:
                if isinstance(step, dict) and "status" not in step:
                    step["status"] = "planned"

            self.state_manager.save_workflow(workflow_dict)
            logger.info(
                "Workflow planned successfully with ID: %s (status=planned, no validation)",
                workflow_id
            )

            return {
                "workflow_id": workflow_id,
                "status": "planned",
                "workflow_name": workflow_name,
                "step_count": len(steps),
            }
        except Exception as e:
            logger.error(f"Workflow planning failed: {e}")
            raise

    def submit_workflow(self, workflow_json: Dict[str, Any], auth_token: str = None) -> Dict[str, str]:
        """Validate and submit a workflow specification.

        This endpoint validates a workflow spec using the same pipeline as the
        validation endpoint, registers it as planned, then submits it.

        Backward compatibility: if payload only contains workflow_id, submission
        is delegated to submit_planned_workflow().

        Args:
            workflow_json: Workflow specification payload or workflow_id-only payload
            auth_token: Optional authorization token to update stored token

        Returns:
            Dictionary with workflow_id and status

        Raises:
            ValueError: If workflow_id is missing or workflow cannot be submitted
            Exception: If submission fails
        """
        try:
            logger.info("Starting workflow submission from workflow spec")
            if not isinstance(workflow_json, dict):
                raise ValueError("submit_workflow requires a JSON object")

            workflow_id = workflow_json.get("workflow_id")
            if workflow_id and "steps" not in workflow_json:
                logger.info(
                    "submit_workflow received workflow_id-only payload; delegating to planned submission"
                )
                return self.submit_planned_workflow(workflow_id, auth_token=auth_token)

            registration = self.register_workflow(workflow_json, auth_token=auth_token)
            return self.submit_planned_workflow(
                registration["workflow_id"],
                auth_token=auth_token
            )

        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Workflow submission failed: {e}")
            raise

    def submit_planned_workflow(self, workflow_id: str, auth_token: str = None) -> Dict[str, str]:
        """Validate and promote a persisted planned workflow to pending execution.

        Args:
            workflow_id: Planned workflow identifier
            auth_token: Optional authorization token to update stored token

        Returns:
            Dictionary with workflow_id and status
        """
        try:
            logger.info(f"Submitting planned workflow {workflow_id}")
            workflow = self.state_manager.get_workflow(workflow_id)
            if not workflow:
                raise ValueError(f"Workflow {workflow_id} not found")

            current_status = workflow.get('status')
            if current_status == 'pending':
                logger.info("Workflow %s is already pending; treating submit as idempotent", workflow_id)
                return {
                    'workflow_id': workflow_id,
                    'status': 'pending'
                }

            if current_status != 'planned':
                raise ValueError(
                    f"Workflow {workflow_id} cannot be submitted from status '{current_status}'"
                )

            # Validation is intentionally done at submission time for planned workflows.
            validation_token = auth_token or workflow.get("auth_token")
            workflow_for_validation = self._sanitize_workflow_for_validation(workflow)
            validated_workflow = self.validator.validate_workflow_input(
                workflow_for_validation,
                auth_token=validation_token
            )
            validated_workflow_dict = validated_workflow.model_dump()

            steps = validated_workflow_dict.get('steps', [])
            for step in steps:
                if isinstance(step, dict):
                    step['status'] = 'pending'

            from models.workflow import ExecutionMetadata
            max_parallel = config.executor.get('max_parallel_steps_per_workflow', 3)
            execution_metadata = ExecutionMetadata(
                total_steps=len(steps),
                completed_steps=0,
                running_steps=0,
                failed_steps=0,
                pending_steps=len(steps),
                currently_running_step_ids=[],
                completed_step_ids=[],
                max_parallel_steps=max_parallel
            ).model_dump()

            log_dir = config.logging.get('workflow_log_dir', 'logs/workflows')
            updates = dict(validated_workflow_dict)
            updates.update({
                'steps': steps,
                'status': 'pending',
                'execution_metadata': execution_metadata,
                'log_file_path': f"{log_dir}/{workflow_id}.log"
            })
            if auth_token:
                updates['auth_token'] = auth_token

            updated = self.state_manager.update_workflow_fields(workflow_id, updates)
            if not updated:
                raise ValueError(f"Workflow {workflow_id} not found")

            logger.info(f"Planned workflow {workflow_id} promoted to pending")
            return {
                'workflow_id': workflow_id,
                'status': 'pending'
            }
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to submit planned workflow {workflow_id}: {e}")
            raise

    @staticmethod
    def _sanitize_workflow_for_validation(workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Remove persistence/runtime fields before validator input checks.
        
        Note: workflow_id is now preserved to support validation of workflows
        that already have an assigned ID.
        """
        payload = json.loads(json.dumps(workflow or {}))

        # workflow_id is intentionally NOT removed - validation now allows it
        for top_level_field in (
            "status",
            "created_at",
            "updated_at",
            "submitted_at",
            "started_at",
            "completed_at",
            "error_message",
            "execution_metadata",
            "log_file_path",
            "auth_token",
        ):
            payload.pop(top_level_field, None)

        if isinstance(payload.get("steps"), list):
            for step in payload["steps"]:
                if not isinstance(step, dict):
                    continue
                for step_field in (
                    "step_id",
                    "status",
                    "task_id",
                    "submitted_at",
                    "started_at",
                    "completed_at",
                    "elapsed_time",
                    "error_message",
                ):
                    step.pop(step_field, None)

        return payload

    def validate_workflow(self, workflow_json: Dict[str, Any], auth_token: str = None) -> Dict[str, Any]:
        """Validate a workflow without submission side effects.

        This runs the same compile/validation pipeline used by submission:
        1. Resolve variable placeholders
        2. Validate workflow schema and business rules
        3. Apply service defaults/normalization

        Unlike submit_workflow(), this method does NOT:
        - assign workflow_id
        - persist to MongoDB
        - mutate execution state

        Args:
            workflow_json: Raw workflow JSON dictionary
            auth_token: Optional authorization token for workspace-dependent checks

        Returns:
            Dictionary with validated workflow and validation metadata

        Raises:
            ValueError: If variable resolution or validation fails
            Exception: If validation pipeline fails unexpectedly
        """
        try:
            logger.info("Starting workflow validation (no submission)")

            # Keep immutable copies for reporting.
            original_workflow = json.loads(json.dumps(workflow_json))

            # Step 1: Clean up empty optional lists before processing
            logger.info("Cleaning up empty optional lists in workflow")
            cleaned_workflow = clean_empty_optional_lists(workflow_json)

            # Step 2: Resolve variable placeholders.
            logger.info("Resolving variable placeholders for validation")
            resolved_workflow = VariableResolver.resolve_workflow_variables(cleaned_workflow)

            # Step 2: Validate workflow and apply service-level normalization/defaults.
            logger.info("Validating workflow")
            validated_workflow = self.validator.validate_workflow_input(
                resolved_workflow,
                auth_token=auth_token
            )
            validated_dict = validated_workflow.model_dump()

            # Surface coarse-grained auto-fix metadata so callers can explain
            # what changed during compile/validation.
            auto_fixes = []
            if original_workflow != resolved_workflow:
                auto_fixes.append("Resolved template variables from base_context/step outputs")
            if resolved_workflow != validated_dict:
                auto_fixes.append("Applied service defaults/normalization during validation")

            return {
                "valid": True,
                "workflow_json": validated_dict,
                "warnings": [],
                "auto_fixes": auto_fixes,
                "message": "Workflow validated successfully"
            }

        except ValueError as e:
            logger.error(f"Workflow validation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Workflow validation pipeline failed: {e}")
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
                # Pending workflows may not have scheduler-assigned step_id/task_id yet.
                # StepStatus requires a string, so coerce null/empty values safely.
                step_id_value = step.get('step_id') or step.get('task_id') or ''
                steps.append(StepStatus(
                    step_id=step_id_value,
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

            # Register first to assign workflow_id, then submit by workflow_id.
            registration = self.register_workflow(custom_workflow, auth_token=auth_token)
            return self.submit_planned_workflow(
                registration["workflow_id"],
                auth_token=auth_token
            )

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

