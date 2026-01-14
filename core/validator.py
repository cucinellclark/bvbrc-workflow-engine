"""Workflow JSON validation and dependency checking."""
from typing import Dict, List, Set, Any
import re

from pydantic import ValidationError
from models.workflow import WorkflowDefinition, WorkflowStep
from utils.logger import get_logger


logger = get_logger(__name__)


class WorkflowValidator:
    """Validates workflow JSON and business logic."""
    
    @staticmethod
    def validate_workflow_input(workflow_data: Dict[str, Any]) -> WorkflowDefinition:
        """Validate workflow input JSON.
        
        Args:
            workflow_data: Raw workflow dictionary
            
        Returns:
            Validated WorkflowDefinition object
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Check that workflow_id is NOT present (input format)
            if 'workflow_id' in workflow_data:
                raise ValueError(
                    "Input workflow should not contain 'workflow_id'. "
                    "IDs are assigned by the scheduler."
                )
            
            # Check that step_id is NOT present in any step
            for step in workflow_data.get('steps', []):
                if 'step_id' in step:
                    raise ValueError(
                        f"Input step '{step.get('step_name')}' should not "
                        f"contain 'step_id'. IDs are assigned by the scheduler."
                    )
            
            # Validate using Pydantic model
            logger.info("Validating workflow schema")
            workflow = WorkflowDefinition(**workflow_data)
            
            # Additional business logic validation
            WorkflowValidator.validate_step_dependencies(workflow.steps)
            WorkflowValidator.validate_variable_references(workflow)
            
            logger.info(
                f"Workflow '{workflow.workflow_name}' validation successful"
            )
            return workflow
            
        except ValidationError as e:
            logger.error(f"Schema validation failed: {e}")
            raise ValueError(f"Schema validation failed: {str(e)}")
        except ValueError as e:
            logger.error(f"Validation failed: {e}")
            raise
    
    @staticmethod
    def validate_step_dependencies(steps: List[WorkflowStep]) -> None:
        """Validate step dependencies.
        
        Checks:
        - All dependencies reference valid step names
        - No circular dependencies exist
        
        Args:
            steps: List of workflow steps
            
        Raises:
            ValueError: If dependencies are invalid
        """
        logger.debug("Validating step dependencies")
        
        # Build step name set
        step_names = {step.step_name for step in steps}
        
        # Check all depends_on references are valid
        for step in steps:
            if step.depends_on:
                for dep in step.depends_on:
                    if dep not in step_names:
                        raise ValueError(
                            f"Step '{step.step_name}' depends on "
                            f"unknown step '{dep}'"
                        )
        
        # Check for circular dependencies using DFS
        WorkflowValidator._check_circular_dependencies(steps)
        
        logger.debug("Step dependencies validated")
    
    @staticmethod
    def _check_circular_dependencies(steps: List[WorkflowStep]) -> None:
        """Check for circular dependencies in workflow steps.
        
        Uses depth-first search to detect cycles.
        
        Args:
            steps: List of workflow steps
            
        Raises:
            ValueError: If circular dependency detected
        """
        # Build dependency graph
        graph: Dict[str, List[str]] = {}
        for step in steps:
            graph[step.step_name] = step.depends_on or []
        
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        
        def dfs(node: str, path: List[str]) -> None:
            """Depth-first search to detect cycles."""
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    dfs(neighbor, path.copy())
                elif neighbor in rec_stack:
                    # Circular dependency found
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    raise ValueError(
                        f"Circular dependency detected: "
                        f"{' -> '.join(cycle)}"
                    )
            
            rec_stack.remove(node)
        
        # Run DFS from each node
        for step_name in graph.keys():
            if step_name not in visited:
                dfs(step_name, [])
    
    @staticmethod
    def validate_variable_references(workflow: WorkflowDefinition) -> None:
        """Validate variable reference syntax.
        
        Checks template strings like:
        - ${workspace_root}
        - ${steps.step_name.outputs.output_name}
        - ${params.param_name}
        
        Args:
            workflow: Workflow definition
            
        Raises:
            ValueError: If variable references are invalid
        """
        logger.debug("Validating variable references")
        
        step_names = {step.step_name for step in workflow.steps}
        
        # Pattern to match variable references
        var_pattern = re.compile(r'\$\{([^}]+)\}')
        
        def check_string(value: str, context: str) -> None:
            """Check a string for valid variable references."""
            matches = var_pattern.findall(value)
            for match in matches:
                parts = match.split('.')
                
                # Check step references
                if len(parts) >= 2 and parts[0] == 'steps':
                    step_ref = parts[1]
                    if step_ref not in step_names:
                        raise ValueError(
                            f"In {context}: Variable reference "
                            f"'${{{match}}}' refers to unknown step "
                            f"'{step_ref}'"
                        )
        
        def check_value(value: Any, context: str) -> None:
            """Recursively check values for variable references."""
            if isinstance(value, str):
                check_string(value, context)
            elif isinstance(value, dict):
                for k, v in value.items():
                    check_value(v, f"{context}.{k}")
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    check_value(item, f"{context}[{i}]")
        
        # Check all steps
        for step in workflow.steps:
            context = f"step '{step.step_name}'"
            
            # Check params
            check_value(step.params, f"{context}.params")
            
            # Check outputs
            if step.outputs:
                check_value(step.outputs, f"{context}.outputs")
        
        # Check workflow outputs
        if workflow.workflow_outputs:
            for i, output in enumerate(workflow.workflow_outputs):
                check_string(output, f"workflow_outputs[{i}]")
        
        logger.debug("Variable references validated")

