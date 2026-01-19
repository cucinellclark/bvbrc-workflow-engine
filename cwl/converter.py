"""CWL to custom workflow format converter."""
from typing import Dict, Any, List, Optional
from pathlib import Path

from cwl.parser import CWLParser
from cwl.tool_mapper import ToolMapper
from cwl.expression_translator import ExpressionTranslator
from utils.logger import get_logger

logger = get_logger(__name__)


class CWLConverter:
    """Converts CWL workflows to custom workflow format."""
    
    def __init__(self, tool_mappings_file: Optional[Path] = None):
        """Initialize CWL converter.
        
        Args:
            tool_mappings_file: Optional path to tool mappings file
        """
        self.parser = CWLParser()
        self.tool_mapper = ToolMapper(tool_mappings_file)
        self.translator = ExpressionTranslator()
    
    def convert(self, cwl_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert CWL workflow to custom format.
        
        Args:
            cwl_data: CWL workflow dictionary
            
        Returns:
            Custom workflow format dictionary
            
        Raises:
            ValueError: If conversion fails
        """
        logger.info("Starting CWL to custom format conversion")
        
        # Validate CWL structure
        self.parser.validate_cwl_workflow(cwl_data)
        
        # Convert workflow metadata
        workflow_name = self._extract_workflow_name(cwl_data)
        version = self._extract_version(cwl_data)
        
        # Convert workflow-level inputs to base_context
        base_context = self._convert_workflow_inputs(cwl_data.get('inputs', {}))
        
        # Convert steps
        steps = self._convert_steps(cwl_data.get('steps', {}))
        
        # Convert workflow outputs
        workflow_outputs = self._convert_workflow_outputs(cwl_data.get('outputs', []))
        
        # Build custom workflow format
        custom_workflow = {
            'workflow_name': workflow_name,
            'version': version,
            'base_context': base_context,
            'steps': steps
        }
        
        if workflow_outputs:
            custom_workflow['workflow_outputs'] = workflow_outputs
        
        logger.info(
            f"Successfully converted CWL workflow '{workflow_name}' "
            f"with {len(steps)} steps"
        )
        
        return custom_workflow
    
    def _extract_workflow_name(self, cwl_data: Dict[str, Any]) -> str:
        """Extract workflow name from CWL data.
        
        Args:
            cwl_data: CWL workflow dictionary
            
        Returns:
            Workflow name
        """
        # Try various fields for workflow name
        if 'label' in cwl_data:
            return str(cwl_data['label'])
        elif 'id' in cwl_data:
            # Extract name from ID (e.g., "workflow.cwl" -> "workflow")
            id_value = str(cwl_data['id'])
            return Path(id_value).stem
        else:
            return "cwl-workflow"
    
    def _extract_version(self, cwl_data: Dict[str, Any]) -> str:
        """Extract version from CWL data.
        
        Args:
            cwl_data: CWL workflow dictionary
            
        Returns:
            Version string
        """
        if 'cwlVersion' in cwl_data:
            return str(cwl_data['cwlVersion'])
        elif 'version' in cwl_data:
            return str(cwl_data['version'])
        else:
            return "1.0"
    
    def _convert_workflow_inputs(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Convert CWL workflow inputs to base_context.
        
        Args:
            inputs: CWL workflow inputs dictionary
            
        Returns:
            base_context dictionary
        """
        base_context = {}
        
        for input_id, input_def in inputs.items():
            # Handle different input definition formats
            if isinstance(input_def, dict):
                # Get default value if present
                default_value = input_def.get('default', input_id)
                # Get type to determine if it's a special input
                input_type = input_def.get('type', 'string')
                
                # For common workflow inputs, map to base_context
                if input_id == 'workspace_output_folder' or 'workspace' in input_id.lower():
                    base_context[input_id] = f"${{{input_id}}}"
                else:
                    # Store as variable reference
                    base_context[input_id] = f"${{{input_id}}}"
            else:
                # Simple value
                base_context[input_id] = input_def
        
        # Ensure base_url and workspace_output_folder if not present
        if 'base_url' not in base_context:
            base_context['base_url'] = "https://www.bv-brc.org"
        
        if 'workspace_output_folder' not in base_context:
            # Try to find a similar field
            for key in base_context.keys():
                if 'workspace' in key.lower() or 'output' in key.lower():
                    base_context['workspace_output_folder'] = base_context[key]
                    break
            else:
                base_context['workspace_output_folder'] = "${workspace_output_folder}"
        
        return base_context
    
    def _convert_steps(self, cwl_steps: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert CWL steps to custom format steps.
        
        Args:
            cwl_steps: CWL steps dictionary (step_name -> step_def)
            
        Returns:
            List of custom format steps
        """
        custom_steps = []
        
        for step_name, step_def in cwl_steps.items():
            if not isinstance(step_def, dict):
                logger.warning(f"Skipping invalid step '{step_name}': not a dictionary")
                continue
            
            # Get tool reference
            tool_ref = step_def.get('run')
            if not tool_ref:
                raise ValueError(f"Step '{step_name}' missing 'run' field (tool reference)")
            
            # Map tool to app name
            if isinstance(tool_ref, str):
                app_name = self.tool_mapper.map_tool_to_app(tool_ref)
            elif isinstance(tool_ref, dict):
                # Inline tool definition
                app_name = self._extract_app_from_inline_tool(tool_ref)
            else:
                raise ValueError(f"Step '{step_name}' has invalid tool reference: {tool_ref}")
            
            # Convert step inputs to params
            step_inputs = step_def.get('in', {})
            params = self._convert_step_inputs(step_inputs)
            
            # Convert step outputs
            step_outputs = step_def.get('out', [])
            outputs = self._convert_step_outputs(step_name, step_outputs, params)
            
            # Extract dependencies from step inputs
            depends_on = self.translator.extract_step_dependencies(step_inputs)
            
            # Build custom step
            custom_step = {
                'step_name': step_name,
                'app': app_name,
                'params': params,
                'outputs': outputs,
                'depends_on': depends_on
            }
            
            custom_steps.append(custom_step)
            logger.debug(
                f"Converted step '{step_name}': app={app_name}, "
                f"dependencies={depends_on}"
            )
        
        return custom_steps
    
    def _extract_app_from_inline_tool(self, tool_def: Dict[str, Any]) -> str:
        """Extract app name from inline CWL tool definition.
        
        Args:
            tool_def: Inline CWL tool dictionary
            
        Returns:
            App name
        """
        # Try to get app name from tool metadata
        if 'label' in tool_def:
            return self.tool_mapper.map_tool_to_app(tool_def['label'])
        elif 'id' in tool_def:
            return self.tool_mapper.map_tool_to_app(tool_def['id'])
        else:
            # Use convention-based conversion
            return "UnknownApp"
    
    def _convert_step_inputs(self, step_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Convert CWL step inputs to params.
        
        Args:
            step_inputs: CWL step inputs dictionary
            
        Returns:
            Params dictionary
        """
        params = {}
        
        for param_name, input_value in step_inputs.items():
            # Convert CWL input value to param value
            converted_value = self._convert_input_value(input_value)
            params[param_name] = converted_value
        
        return params
    
    def _convert_input_value(self, value: Any) -> Any:
        """Convert a CWL input value to custom format.
        
        Args:
            value: CWL input value (may be expression, file object, or primitive)
            
        Returns:
            Converted value
        """
        if isinstance(value, str):
            # Translate CWL expressions
            return self.translator.translate_expression(value)
        
        elif isinstance(value, dict):
            # Handle CWL file objects or complex types
            if 'path' in value:
                # File object - extract path
                return value['path']
            elif 'location' in value:
                # File location
                return value['location']
            else:
                # Recursively convert nested structures
                return {
                    k: self._convert_input_value(v)
                    for k, v in value.items()
                }
        
        elif isinstance(value, list):
            # Convert list items
            return [self._convert_input_value(item) for item in value]
        
        else:
            # Primitive type - return as-is
            return value
    
    def _convert_step_outputs(
        self,
        step_name: str,
        step_outputs: List[str],
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Convert CWL step outputs to custom format.
        
        Args:
            step_name: Step name
            step_outputs: List of CWL output IDs
            params: Step params (for generating output paths)
            
        Returns:
            Outputs dictionary
        """
        outputs = {}
        
        for output_id in step_outputs:
            # Generate output path using convention
            # Try to use output_path from params if available
            if 'output_path' in params:
                base_path = params['output_path']
                if isinstance(base_path, str):
                    # Remove ${} wrapper if present for path construction
                    clean_path = base_path.replace('${', '').replace('}', '')
                    output_path = f"${{params.output_path}}/{output_id}"
                else:
                    output_path = f"{base_path}/{output_id}"
            else:
                # Use default pattern
                output_path = f"${{params.output_path}}/{output_id}"
            
            outputs[output_id] = output_path
        
        return outputs
    
    def _convert_workflow_outputs(self, cwl_outputs: List[Any]) -> List[str]:
        """Convert CWL workflow outputs to custom format.
        
        Args:
            cwl_outputs: CWL workflow outputs list
            
        Returns:
            List of workflow output references
        """
        workflow_outputs = []
        
        for output_def in cwl_outputs:
            if isinstance(output_def, str):
                # Simple output ID - convert to step reference format
                workflow_outputs.append(output_def)
            elif isinstance(output_def, dict):
                # Output definition with source
                output_id = output_def.get('id', '')
                source = output_def.get('outputSource', '')
                
                if source:
                    # Convert CWL source format to custom format
                    # e.g., "steps.step_name.output_name" -> "${steps.step_name.outputs.output_name}"
                    converted = self.translator.translate_expression(f"$({source})")
                    workflow_outputs.append(converted)
                elif output_id:
                    workflow_outputs.append(output_id)
        
        return workflow_outputs

