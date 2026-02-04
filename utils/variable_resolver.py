"""Variable resolver for workflow JSON placeholders."""
import re
from typing import Dict, Any, Union, List
from utils.logger import get_logger


logger = get_logger(__name__)


class VariableResolver:
    """Resolves variable placeholders in workflow JSON using multi-pass resolution.
    
    Resolution happens in three passes:
    1. Base context variables: ${workspace_output_folder} -> actual path
    2. Params references: ${params.output_path} -> resolved params value (within each step)
    3. Step outputs: ${steps.step_name.outputs.output_name} -> actual file paths
    """
    
    # Pattern to match variable references like ${variable_name}
    VAR_PATTERN = re.compile(r'\$\{([^}]+)\}')
    
    # Pattern to match simple variable names (single identifier, no dots/brackets)
    SIMPLE_VAR_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    
    # Pattern to match params references: ${params.param_name}
    PARAMS_PATTERN = re.compile(r'^\$\{params\.([a-zA-Z_][a-zA-Z0-9_]*)\}$')
    
    # Pattern to match step output references: ${steps.step_name.outputs.output_name}
    STEP_OUTPUT_PATTERN = re.compile(r'^\$\{steps\.([a-zA-Z_][a-zA-Z0-9_]*)\.outputs\.([a-zA-Z_][a-zA-Z0-9_]*)\}$')
    
    @staticmethod
    def resolve_workflow_variables(workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve all variable placeholders in workflow JSON using multi-pass resolution.
        
        Resolution happens in three passes:
        1. Base context variables: ${workspace_output_folder} -> actual path from base_context
        2. Params references: ${params.output_path} -> resolved params value (within each step)
        3. Step outputs: ${steps.step_name.outputs.output_name} -> actual file paths
        
        Args:
            workflow_data: Raw workflow dictionary with placeholders
            
        Returns:
            Workflow dictionary with all placeholders resolved
            
        Raises:
            ValueError: If a required variable cannot be resolved
        """
        logger.info("Starting multi-pass variable resolution")
        
        # Make a deep copy to avoid modifying the original
        resolved_workflow = VariableResolver._deep_copy(workflow_data)
        
        # PASS 1: Resolve base_context variables
        logger.info("Pass 1: Resolving base_context variables")
        resolved_workflow = VariableResolver._resolve_base_context_variables(resolved_workflow)
        
        # PASS 2: Resolve params references within each step
        logger.info("Pass 2: Resolving params references within each step")
        resolved_workflow = VariableResolver._resolve_params_references(resolved_workflow)
        
        # PASS 3: Resolve step output references
        logger.info("Pass 3: Resolving step output references")
        resolved_workflow = VariableResolver._resolve_step_output_references(resolved_workflow)
        
        logger.info("Multi-pass variable resolution completed successfully")
        return resolved_workflow
    
    @staticmethod
    def _deep_copy(obj: Any) -> Any:
        """Create a deep copy of an object."""
        if isinstance(obj, dict):
            return {key: VariableResolver._deep_copy(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [VariableResolver._deep_copy(item) for item in obj]
        else:
            return obj
    
    @staticmethod
    def _resolve_base_context_variables(workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """Pass 1: Resolve base_context variables like ${workspace_output_folder}.
        
        Args:
            workflow_data: Workflow dictionary
            
        Returns:
            Workflow dictionary with base_context variables resolved
        """
        # Extract base_context for variable resolution
        base_context = workflow_data.get('base_context', {})
        
        # Build variable lookup dictionary
        variables = {}
        if base_context:
            for key, value in base_context.items():
                variables[key] = value
                logger.debug(f"Registered base_context variable: {key} = {value}")
        
        # Resolve variables recursively
        return VariableResolver._resolve_simple_variables_recursive(
            workflow_data,
            variables,
            context_path="workflow"
        )
    
    @staticmethod
    def _resolve_params_references(workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """Pass 2: Resolve params references like ${params.output_path} within each step.
        
        Args:
            workflow_data: Workflow dictionary
            
        Returns:
            Workflow dictionary with params references resolved
        """
        steps = workflow_data.get('steps', [])
        
        for step_idx, step in enumerate(steps):
            step_name = step.get('step_name', f'step_{step_idx}')
            params = step.get('params', {})
            outputs = step.get('outputs', {})
            
            # Resolve params references in outputs section
            if outputs:
                logger.debug(f"Resolving params references in step '{step_name}' outputs")
                resolved_outputs = VariableResolver._resolve_params_in_dict(
                    outputs, 
                    params,
                    f"step '{step_name}'.outputs"
                )
                step['outputs'] = resolved_outputs
        
        return workflow_data
    
    @staticmethod
    def _resolve_step_output_references(workflow_data: Dict[str, Any]) -> Dict[str, Any]:
        """Pass 3: Resolve step output references like ${steps.step_name.outputs.output_name}.
        
        Args:
            workflow_data: Workflow dictionary
            
        Returns:
            Workflow dictionary with step output references resolved
        """
        steps = workflow_data.get('steps', [])
        
        # Build step outputs lookup: {step_name: {output_name: value}}
        step_outputs_map = {}
        for step in steps:
            step_name = step.get('step_name')
            outputs = step.get('outputs', {})
            if step_name:
                step_outputs_map[step_name] = outputs
                logger.debug(f"Registered step '{step_name}' outputs: {list(outputs.keys())}")
        
        # Resolve step output references in workflow_outputs
        workflow_outputs = workflow_data.get('workflow_outputs', [])
        if workflow_outputs:
            logger.debug("Resolving step output references in workflow_outputs")
            resolved_outputs = []
            for i, output_ref in enumerate(workflow_outputs):
                resolved = VariableResolver._resolve_step_output_ref(
                    output_ref,
                    step_outputs_map,
                    f"workflow_outputs[{i}]"
                )
                resolved_outputs.append(resolved)
            workflow_data['workflow_outputs'] = resolved_outputs
        
        return workflow_data
    
    @staticmethod
    def _resolve_params_in_dict(
        obj: Any,
        params: Dict[str, Any],
        context_path: str
    ) -> Any:
        """Recursively resolve params references in an object.
        
        Args:
            obj: Object to resolve
            params: Dictionary of parameter values
            context_path: Path for error reporting
            
        Returns:
            Object with params references resolved
        """
        if isinstance(obj, str):
            return VariableResolver._resolve_params_in_string(obj, params, context_path)
        elif isinstance(obj, dict):
            return {
                key: VariableResolver._resolve_params_in_dict(
                    value, params, f"{context_path}.{key}"
                )
                for key, value in obj.items()
            }
        elif isinstance(obj, list):
            return [
                VariableResolver._resolve_params_in_dict(
                    item, params, f"{context_path}[{i}]"
                )
                for i, item in enumerate(obj)
            ]
        else:
            return obj
    
    @staticmethod
    def _resolve_params_in_string(
        value: str,
        params: Dict[str, Any],
        context_path: str
    ) -> str:
        """Resolve params references in a string.
        
        Args:
            value: String potentially containing ${params.xxx} references
            params: Dictionary of parameter values
            context_path: Path for error reporting
            
        Returns:
            String with params references resolved
        """
        if not isinstance(value, str):
            return value
        
        # Find all variable references
        matches = VariableResolver.VAR_PATTERN.findall(value)
        
        if not matches:
            return value
        
        resolved = value
        for match in matches:
            # Check if this is a params reference
            if match.startswith('params.'):
                param_name = match[7:]  # Remove 'params.' prefix
                
                if param_name in params:
                    param_value = str(params[param_name])
                    resolved = resolved.replace(f"${{{match}}}", param_value)
                    logger.debug(
                        f"Resolved {context_path}: ${{{match}}} -> {param_value}"
                    )
                else:
                    error_msg = (
                        f"Cannot resolve params reference '${{{match}}}' in {context_path}. "
                        f"Parameter '{param_name}' not found in step params."
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)
        
        return resolved
    
    @staticmethod
    def _resolve_step_output_ref(
        value: str,
        step_outputs_map: Dict[str, Dict[str, Any]],
        context_path: str
    ) -> str:
        """Resolve step output reference to actual value.
        
        Resolves ${steps.step_name.outputs.output_name} to the actual output value.
        
        Args:
            value: String containing step output reference
            step_outputs_map: Map of {step_name: {output_name: value}}
            context_path: Path for error reporting
            
        Returns:
            Resolved value
        """
        if not isinstance(value, str):
            return value
        
        # Check if this is a step output reference
        match = VariableResolver.STEP_OUTPUT_PATTERN.match(value)
        if match:
            step_name = match.group(1)
            output_name = match.group(2)
            
            # Look up the step
            if step_name not in step_outputs_map:
                error_msg = (
                    f"Cannot resolve step reference '${{{value}}}' in {context_path}. "
                    f"Step '{step_name}' not found."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Look up the output
            step_outputs = step_outputs_map[step_name]
            if output_name not in step_outputs:
                error_msg = (
                    f"Cannot resolve step reference '${{{value}}}' in {context_path}. "
                    f"Output '{output_name}' not found in step '{step_name}' outputs."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            resolved_value = step_outputs[output_name]
            logger.debug(
                f"Resolved {context_path}: {value} -> {resolved_value}"
            )
            return resolved_value
        
        # Not a step output reference, return as-is
        return value
    
    @staticmethod
    def _resolve_simple_variables_recursive(
        obj: Any,
        variables: Dict[str, Any],
        context_path: str = ""
    ) -> Any:
        """Recursively resolve simple base_context variables.
        
        Args:
            obj: Object to resolve (dict, list, str, or primitive)
            variables: Dictionary of variable names to values
            context_path: Path for error reporting
            
        Returns:
            Object with variables resolved
        """
        if isinstance(obj, str):
            return VariableResolver._resolve_simple_variables_in_string(
                obj, variables, context_path
            )
        elif isinstance(obj, dict):
            resolved = {}
            for key, value in obj.items():
                # Skip resolving base_context itself to avoid circular issues
                if key == 'base_context':
                    resolved[key] = value
                else:
                    resolved[key] = VariableResolver._resolve_simple_variables_recursive(
                        value,
                        variables,
                        f"{context_path}.{key}" if context_path else key
                    )
            return resolved
        elif isinstance(obj, list):
            return [
                VariableResolver._resolve_simple_variables_recursive(
                    item,
                    variables,
                    f"{context_path}[{i}]" if context_path else f"[{i}]"
                )
                for i, item in enumerate(obj)
            ]
        else:
            # Primitive type (int, float, bool, None) - return as-is
            return obj
    
    @staticmethod
    def _resolve_simple_variables_in_string(
        value: str,
        variables: Dict[str, Any],
        context_path: str
    ) -> str:
        """Resolve simple variable placeholders in a string.
        
        Only resolves simple variables (single identifier).
        Skips params references and step output references.
        
        Args:
            value: String potentially containing placeholders
            variables: Dictionary of variable names to values
            context_path: Path for error reporting
            
        Returns:
            String with variables resolved
        """
        if not isinstance(value, str):
            return value
        
        # Find all variable references
        matches = VariableResolver.VAR_PATTERN.findall(value)
        
        if not matches:
            return value
        
        resolved = value
        for var_name in matches:
            # Only resolve simple variables (single identifier, no dots/brackets/quotes)
            # Skip params references (params.xxx) and step references (steps.xxx)
            if not VariableResolver.SIMPLE_VAR_PATTERN.match(var_name):
                # This is a complex reference - skip for now
                logger.debug(
                    f"Skipping complex variable reference '${{{var_name}}}' in {context_path} "
                    f"(will be resolved in later passes)"
                )
                continue
            
            # Check if variable exists
            if var_name in variables:
                var_value = str(variables[var_name])
                resolved = resolved.replace(f"${{{var_name}}}", var_value)
                logger.debug(
                    f"Resolved {context_path}: ${{{var_name}}} -> {var_value}"
                )
            else:
                # Try environment variable as fallback
                import os
                env_value = os.getenv(var_name)
                if env_value:
                    resolved = resolved.replace(f"${{{var_name}}}", env_value)
                    logger.debug(
                        f"Resolved {context_path} from environment: "
                        f"${{{var_name}}} -> {env_value}"
                    )
                else:
                    error_msg = (
                        f"Cannot resolve variable '${{{var_name}}}' in {context_path}. "
                        f"Variable not found in base_context or environment variables."
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)
        
        return resolved
    
    @staticmethod
    def resolve_step_params_runtime(
        step_params: Dict[str, Any],
        workflow_steps: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Dynamically resolve step output and param references in step parameters at runtime.
        
        This is called right before submitting a step to the scheduler, to resolve
        references to outputs and params from previously completed steps.
        
        Supports:
        - ${steps.xxx.outputs.yyy} - references to step outputs
        - ${steps.xxx.params.yyy} - references to step params
        
        Args:
            step_params: Step parameters that may contain ${steps.xxx.outputs.yyy} or ${steps.xxx.params.yyy} references
            workflow_steps: All workflow steps with their current state, outputs, and params
            
        Returns:
            Parameters with step references resolved to actual values
        """
        # Build step outputs map from current workflow state
        step_outputs_map = {}
        step_params_map = {}
        for step in workflow_steps:
            step_name = step.get('step_name')
            if step_name:
                outputs = step.get('outputs', {})
                if outputs:
                    step_outputs_map[step_name] = outputs
                params = step.get('params', {})
                if params:
                    step_params_map[step_name] = params
        
        # Recursively resolve references in params
        return VariableResolver._resolve_step_references_in_dict(
            step_params,
            step_outputs_map,
            step_params_map,
            "step_params"
        )
    
    @staticmethod
    def _resolve_step_references_in_dict(
        obj: Any,
        step_outputs_map: Dict[str, Dict[str, Any]],
        step_params_map: Dict[str, Dict[str, Any]],
        context_path: str
    ) -> Any:
        """Recursively resolve step output and param references in an object.
        
        Args:
            obj: Object to resolve
            step_outputs_map: Map of {step_name: {output_name: value}}
            step_params_map: Map of {step_name: {param_name: value}}
            context_path: Path for error reporting
            
        Returns:
            Object with step references resolved
        """
        if isinstance(obj, str):
            return VariableResolver._resolve_step_references_in_string(
                obj, step_outputs_map, step_params_map, context_path
            )
        elif isinstance(obj, dict):
            return {
                key: VariableResolver._resolve_step_references_in_dict(
                    value, step_outputs_map, step_params_map, f"{context_path}.{key}"
                )
                for key, value in obj.items()
            }
        elif isinstance(obj, list):
            return [
                VariableResolver._resolve_step_references_in_dict(
                    item, step_outputs_map, step_params_map, f"{context_path}[{i}]"
                )
                for i, item in enumerate(obj)
            ]
        else:
            return obj
    
    @staticmethod
    def _resolve_step_references_in_string(
        value: str,
        step_outputs_map: Dict[str, Dict[str, Any]],
        step_params_map: Dict[str, Dict[str, Any]],
        context_path: str
    ) -> str:
        """Resolve step output and param references in a string.
        
        Resolves:
        - ${steps.step_name.outputs.output_name} patterns
        - ${steps.step_name.params.param_name} patterns
        
        Args:
            value: String potentially containing step references
            step_outputs_map: Map of {step_name: {output_name: value}}
            step_params_map: Map of {step_name: {param_name: value}}
            context_path: Path for error reporting
            
        Returns:
            String with step references resolved
        """
        if not isinstance(value, str):
            return value
        
        # Find all variable references
        matches = VariableResolver.VAR_PATTERN.findall(value)
        
        if not matches:
            return value
        
        resolved = value
        for match in matches:
            # Check if this is a step reference: steps.step_name.outputs.output_name or steps.step_name.params.param_name
            if match.startswith('steps.'):
                parts = match.split('.')
                # Pattern: steps.step_name.outputs.output_name or steps.step_name.params.param_name
                if len(parts) == 4 and parts[0] == 'steps':
                    step_name = parts[1]
                    ref_type = parts[2]  # 'outputs' or 'params'
                    ref_name = parts[3]
                    
                    if ref_type == 'outputs':
                        # Look up the step outputs
                        if step_name not in step_outputs_map:
                            logger.warning(
                                f"Cannot resolve step reference '${{{match}}}' in {context_path}. "
                                f"Step '{step_name}' outputs not available yet."
                            )
                            continue  # Leave unresolved - step may not have run yet
                        
                        # Look up the output
                        step_outputs = step_outputs_map[step_name]
                        if ref_name not in step_outputs:
                            logger.warning(
                                f"Cannot resolve step reference '${{{match}}}' in {context_path}. "
                                f"Output '{ref_name}' not found in step '{step_name}' outputs."
                            )
                            continue  # Leave unresolved
                        
                        resolved_value = str(step_outputs[ref_name])
                        resolved = resolved.replace(f"${{{match}}}", resolved_value)
                        logger.info(
                            f"Runtime resolved {context_path}: ${{{match}}} -> {resolved_value}"
                        )
                    
                    elif ref_type == 'params':
                        # Look up the step params
                        if step_name not in step_params_map:
                            logger.warning(
                                f"Cannot resolve step reference '${{{match}}}' in {context_path}. "
                                f"Step '{step_name}' params not available."
                            )
                            continue  # Leave unresolved
                        
                        # Look up the param
                        step_params = step_params_map[step_name]
                        if ref_name not in step_params:
                            logger.warning(
                                f"Cannot resolve step reference '${{{match}}}' in {context_path}. "
                                f"Param '{ref_name}' not found in step '{step_name}' params."
                            )
                            continue  # Leave unresolved
                        
                        resolved_value = str(step_params[ref_name])
                        resolved = resolved.replace(f"${{{match}}}", resolved_value)
                        logger.info(
                            f"Runtime resolved {context_path}: ${{{match}}} -> {resolved_value}"
                        )
        
        return resolved

