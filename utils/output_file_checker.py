"""Output file conflict checker and resolver.

This module checks if workflow step output files already exist in the workspace
and automatically generates unique names to prevent conflicts.
"""
import re
import os
from typing import Dict, Any, Optional, Tuple
from utils.logger import get_logger

logger = get_logger(__name__)


class OutputFileChecker:
    """Checks and resolves output file conflicts in workflow definitions."""
    
    # Pattern to match simple variable references (for base_context resolution)
    SIMPLE_VAR_PATTERN = re.compile(r'\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}')
    
    def __init__(self, auth_token: str):
        """Initialize the output file checker.
        
        Args:
            auth_token: Authentication token for workspace API calls
        """
        self.auth_token = auth_token
        self.workspace_client = None
        self._init_workspace_client()
    
    def _init_workspace_client(self) -> None:
        """Initialize workspace client with auth token."""
        try:
            # Import workspace client from bvbrc_groups_module
            import sys
            from pathlib import Path
            
            # Add bvbrc_groups_module to path if not already there
            bvbrc_module_path = Path(__file__).parent.parent.parent / 'bvbrc_groups_module'
            if str(bvbrc_module_path) not in sys.path:
                sys.path.insert(0, str(bvbrc_module_path))
            
            from utils.workspace_client import WorkspaceClient
            
            self.workspace_client = WorkspaceClient(token=self.auth_token)
            logger.info("Workspace client initialized for output file checking")
            
        except Exception as e:
            logger.error(f"Failed to initialize workspace client: {e}")
            # Set to None - we'll skip checks if client unavailable
            self.workspace_client = None
    
    def check_and_resolve_conflicts(
        self, 
        workflow_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check for output file conflicts and resolve them by renaming.
        
        This is the main entry point for the output file checker.
        
        Args:
            workflow_data: Workflow definition dictionary
            
        Returns:
            Modified workflow_data with resolved output file names
        """
        if not self.workspace_client:
            logger.warning(
                "Workspace client not available - skipping output file conflict check"
            )
            return workflow_data
        
        logger.info("Checking for output file conflicts")
        
        base_context = workflow_data.get('base_context', {})
        steps = workflow_data.get('steps', [])
        
        modifications_made = False
        
        for step_idx, step in enumerate(steps):
            step_name = step.get('step_name', f'step_{step_idx}')
            
            if not self._is_relevant_step(step):
                logger.debug(
                    f"Step '{step_name}': Skipping (no output_path/output_file)"
                )
                continue
            
            # Try to resolve conflicts for this step
            try:
                modified = self._resolve_step_output_conflict(
                    step, 
                    base_context, 
                    step_name
                )
                if modified:
                    modifications_made = True
                    
            except Exception as e:
                logger.error(
                    f"Step '{step_name}': Error checking output conflicts: {e}"
                )
                # Continue with other steps
        
        if modifications_made:
            logger.info("Output file conflicts resolved successfully")
        else:
            logger.info("No output file conflicts detected")
        
        return workflow_data
    
    def _is_relevant_step(self, step: Dict[str, Any]) -> bool:
        """Check if step has output_path and output_file parameters.
        
        Args:
            step: Step dictionary
            
        Returns:
            True if step should be checked for conflicts
        """
        params = step.get('params', {})
        return 'output_path' in params and 'output_file' in params
    
    def _resolve_step_output_conflict(
        self,
        step: Dict[str, Any],
        base_context: Dict[str, Any],
        step_name: str
    ) -> bool:
        """Resolve output conflict for a single step.
        
        Args:
            step: Step dictionary (will be modified in place)
            base_context: Base context for variable resolution
            step_name: Step name for logging
            
        Returns:
            True if output_file was modified
        """
        params = step.get('params', {})
        output_path = params.get('output_path', '')
        output_file = params.get('output_file', '')
        
        # Try to resolve output_path variables
        resolved_path, could_resolve = self._resolve_output_path_variables(
            output_path, 
            base_context
        )
        
        if not could_resolve:
            logger.warning(
                f"Step '{step_name}': Cannot fully resolve output_path "
                f"'{output_path}' - skipping conflict check"
            )
            return False
        
        logger.debug(
            f"Step '{step_name}': Checking output path '{resolved_path}/{output_file}'"
        )
        
        # Check if output already exists
        if self._check_output_exists(resolved_path, output_file):
            logger.info(
                f"Step '{step_name}': Output file conflict detected for '{output_file}'"
            )
            
            # Generate unique name
            unique_name = self._generate_unique_output_name(
                output_file,
                resolved_path,
                step_name
            )
            
            if unique_name != output_file:
                logger.info(
                    f"Step '{step_name}': Renamed '{output_file}' → '{unique_name}'"
                )
                params['output_file'] = unique_name
                return True
        else:
            logger.debug(
                f"Step '{step_name}': No conflict for '{output_file}'"
            )
        
        return False
    
    def _resolve_output_path_variables(
        self,
        output_path: str,
        base_context: Dict[str, Any]
    ) -> Tuple[str, bool]:
        """Resolve base_context variables in output_path.
        
        Only resolves simple variables from base_context (like ${workspace_output_folder}).
        Does not resolve ${params.*} or ${steps.*} references.
        
        Args:
            output_path: Output path string with potential variables
            base_context: Base context dictionary
            
        Returns:
            Tuple of (resolved_path, could_resolve_all)
            - resolved_path: Path with simple variables resolved
            - could_resolve_all: False if unresolved variables remain
        """
        if not isinstance(output_path, str):
            return str(output_path), True
        
        resolved = output_path
        could_resolve_all = True
        
        # Find all variable references
        matches = self.SIMPLE_VAR_PATTERN.findall(resolved)
        
        for var_name in matches:
            # Check if this is a simple variable (not params.* or steps.*)
            if '.' in var_name or '[' in var_name:
                # Complex reference - can't resolve yet
                could_resolve_all = False
                continue
            
            # Try to resolve from base_context
            if var_name in base_context:
                var_value = str(base_context[var_name])
                resolved = resolved.replace(f"${{{var_name}}}", var_value)
                logger.debug(f"Resolved ${{{var_name}}} -> {var_value}")
            else:
                # Try environment variable
                env_value = os.getenv(var_name)
                if env_value:
                    resolved = resolved.replace(f"${{{var_name}}}", env_value)
                    logger.debug(f"Resolved ${{{var_name}}} from env -> {env_value}")
                else:
                    # Can't resolve this variable
                    could_resolve_all = False
        
        return resolved, could_resolve_all
    
    def _check_output_exists(
        self,
        output_path: str,
        output_file: str
    ) -> bool:
        """Check if output file/directory exists in workspace.
        
        Checks both:
        - Job result path: {output_path}/{output_file}
        - Hidden directory: {output_path}/.{output_file}
        
        Args:
            output_path: Resolved output path
            output_file: Output file name
            
        Returns:
            True if output exists
        """
        if not self.workspace_client:
            return False
        
        try:
            # Construct full path
            full_path = f"{output_path}/{output_file}".replace('//', '/')
            hidden_path = f"{output_path}/.{output_file}".replace('//', '/')
            
            # Try to get metadata for the job result path
            try:
                metadata = self.workspace_client.get_file_metadata(full_path)
                if metadata:
                    logger.debug(f"Found existing output at: {full_path}")
                    return True
            except Exception as e:
                logger.debug(f"Path {full_path} does not exist: {e}")
            
            # Try to get metadata for the hidden directory
            try:
                metadata = self.workspace_client.get_file_metadata(hidden_path)
                if metadata:
                    logger.debug(f"Found existing output at: {hidden_path}")
                    return True
            except Exception as e:
                logger.debug(f"Path {hidden_path} does not exist: {e}")
            
            # Neither path exists
            return False
            
        except Exception as e:
            logger.warning(f"Error checking output existence: {e}")
            # On error, assume it doesn't exist (fail-open)
            return False
    
    def _generate_unique_output_name(
        self,
        base_name: str,
        output_path: str,
        step_name: str
    ) -> str:
        """Generate a unique output name by appending numbers.
        
        Pattern: base_name, base_name_2, base_name_3, ...
        
        Args:
            base_name: Original output file name
            output_path: Resolved output path
            step_name: Step name for logging
            
        Returns:
            Unique output file name
        """
        max_attempts = int(os.getenv('MAX_OUTPUT_FILE_ATTEMPTS', '100'))
        
        # Try base_name_2, base_name_3, etc.
        for attempt in range(2, max_attempts + 2):
            candidate_name = f"{base_name}_{attempt}"
            
            if not self._check_output_exists(output_path, candidate_name):
                logger.debug(
                    f"Step '{step_name}': Found unique name '{candidate_name}' "
                    f"after {attempt - 1} attempts"
                )
                return candidate_name
        
        # Max attempts exceeded - return the last candidate and let it fail later
        logger.error(
            f"Step '{step_name}': Could not find unique output name after "
            f"{max_attempts} attempts"
        )
        raise ValueError(
            f"Cannot find unique output name for step '{step_name}' "
            f"after {max_attempts} attempts"
        )


def check_and_resolve_output_conflicts(
    workflow_data: Dict[str, Any],
    auth_token: str
) -> Dict[str, Any]:
    """Convenience function to check and resolve output conflicts.
    
    Args:
        workflow_data: Workflow definition dictionary
        auth_token: Authentication token for workspace API calls
        
    Returns:
        Modified workflow_data with resolved conflicts
    """
    # Check if feature is enabled
    check_enabled = os.getenv('CHECK_OUTPUT_FILE_CONFLICTS', 'true').lower()
    if check_enabled not in ('true', '1', 'yes'):
        logger.info("Output file conflict checking is disabled")
        return workflow_data
    
    if not auth_token:
        logger.warning(
            "No auth token provided - skipping output file conflict check"
        )
        return workflow_data
    
    checker = OutputFileChecker(auth_token)
    return checker.check_and_resolve_conflicts(workflow_data)

