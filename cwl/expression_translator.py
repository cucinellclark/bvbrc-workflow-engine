"""Expression translator for converting CWL expressions to custom format."""
import re
from typing import Dict, Any, List, Set
from utils.logger import get_logger

logger = get_logger(__name__)


class ExpressionTranslator:
    """Translates CWL expressions to custom workflow format."""
    
    # Pattern to match CWL expressions: $(...)
    CWL_EXPRESSION_PATTERN = re.compile(r'\$\(([^)]+)\)')
    
    # Pattern to match step output references: steps.step_name.output_name
    STEP_OUTPUT_PATTERN = re.compile(r'^steps\.([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)$')
    
    # Pattern to match input references: inputs.input_name
    INPUT_PATTERN = re.compile(r'^inputs\.([a-zA-Z_][a-zA-Z0-9_]*)$')
    
    @staticmethod
    def translate_expression(cwl_expr: str, context: Dict[str, Any] = None) -> str:
        """Translate CWL expression to custom format.
        
        Args:
            cwl_expr: CWL expression string (may contain $(...))
            context: Optional context for resolving references
            
        Returns:
            Translated expression in custom format
        """
        if not isinstance(cwl_expr, str):
            return cwl_expr
        
        # Find all CWL expressions
        matches = ExpressionTranslator.CWL_EXPRESSION_PATTERN.findall(cwl_expr)
        
        if not matches:
            return cwl_expr
        
        translated = cwl_expr
        for match in matches:
            translated_expr = ExpressionTranslator._translate_single_expression(match)
            translated = translated.replace(f"$({match})", translated_expr)
        
        return translated
    
    @staticmethod
    def _translate_single_expression(expr: str) -> str:
        """Translate a single CWL expression.
        
        Args:
            expr: CWL expression content (without $(...))
            
        Returns:
            Translated expression in custom format
        """
        expr = expr.strip()
        
        # Check for step output reference: steps.step_name.output_name
        step_match = ExpressionTranslator.STEP_OUTPUT_PATTERN.match(expr)
        if step_match:
            step_name = step_match.group(1)
            output_name = step_match.group(2)
            return f"${{steps.{step_name}.outputs.{output_name}}}"
        
        # Check for input reference: inputs.input_name
        input_match = ExpressionTranslator.INPUT_PATTERN.match(expr)
        if input_match:
            input_name = input_match.group(1)
            # Inputs become params in step context, or base_context at workflow level
            return f"${{{input_name}}}"
        
        # Check for self reference (workflow-level input)
        if expr.startswith('self.'):
            input_name = expr[5:]  # Remove 'self.' prefix
            return f"${{{input_name}}}"
        
        # For other expressions, try to convert common patterns
        # If it's just a variable name, wrap it
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', expr):
            return f"${{{expr}}}"
        
        # Complex expression - log warning and try to convert
        logger.warning(
            f"Complex CWL expression detected: $({expr}). "
            "May need manual review after conversion."
        )
        # Try to convert $(...) to ${...} format
        return f"${{{expr}}}"
    
    @staticmethod
    def extract_step_dependencies(step_inputs: Dict[str, Any]) -> List[str]:
        """Extract step dependencies from CWL step inputs.
        
        Args:
            step_inputs: CWL step inputs dictionary
            
        Returns:
            List of step names this step depends on
        """
        dependencies: Set[str] = set()
        
        for input_value in step_inputs.values():
            deps = ExpressionTranslator._extract_dependencies_from_value(input_value)
            dependencies.update(deps)
        
        return sorted(list(dependencies))
    
    @staticmethod
    def _extract_dependencies_from_value(value: Any) -> List[str]:
        """Recursively extract step dependencies from a value.
        
        Args:
            value: Value that may contain step references
            
        Returns:
            List of step names referenced
        """
        dependencies: Set[str] = set()
        
        if isinstance(value, str):
            # Check for step references in string
            matches = ExpressionTranslator.CWL_EXPRESSION_PATTERN.findall(value)
            for match in matches:
                step_match = ExpressionTranslator.STEP_OUTPUT_PATTERN.match(match.strip())
                if step_match:
                    step_name = step_match.group(1)
                    dependencies.add(step_name)
        
        elif isinstance(value, dict):
            for v in value.values():
                deps = ExpressionTranslator._extract_dependencies_from_value(v)
                dependencies.update(deps)
        
        elif isinstance(value, list):
            for item in value:
                deps = ExpressionTranslator._extract_dependencies_from_value(item)
                dependencies.update(deps)
        
        return list(dependencies)

