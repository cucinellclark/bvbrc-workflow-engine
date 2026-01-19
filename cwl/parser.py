"""CWL parser for loading and validating CWL workflow files."""
import json
import yaml
from typing import Dict, Any, Optional, Union
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)


class CWLParser:
    """Parser for CWL workflow files."""
    
    @staticmethod
    def detect_cwl_format(data: Dict[str, Any]) -> bool:
        """Detect if the data is in CWL format.
        
        Args:
            data: Dictionary to check
            
        Returns:
            True if data appears to be CWL format
        """
        # Check for CWL indicators
        if 'class' in data:
            if data.get('class') in ['Workflow', 'CommandLineTool']:
                return True
        
        if 'cwlVersion' in data:
            return True
        
        # Check for CWL workflow structure
        if 'steps' in data and isinstance(data['steps'], dict):
            # CWL workflows have steps as a dict, not a list
            return True
        
        return False
    
    @staticmethod
    def parse_cwl(data: Union[str, Dict[str, Any], Path]) -> Dict[str, Any]:
        """Parse CWL workflow from various input formats.
        
        Args:
            data: CWL data as string (YAML/JSON), dict, or file path
            
        Returns:
            Parsed CWL workflow dictionary
            
        Raises:
            ValueError: If parsing fails or invalid CWL format
        """
        if isinstance(data, Path):
            return CWLParser._parse_file(data)
        elif isinstance(data, str):
            # Try to parse as YAML first, then JSON
            try:
                return yaml.safe_load(data)
            except yaml.YAMLError:
                try:
                    return json.loads(data)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Failed to parse CWL data: {e}")
        elif isinstance(data, dict):
            return data
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
    
    @staticmethod
    def _parse_file(file_path: Path) -> Dict[str, Any]:
        """Parse CWL file from disk.
        
        Args:
            file_path: Path to CWL file
            
        Returns:
            Parsed CWL workflow dictionary
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise ValueError(f"CWL file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            if file_path.suffix in ['.yaml', '.yml']:
                return yaml.safe_load(f)
            elif file_path.suffix == '.json':
                return json.load(f)
            else:
                # Try YAML first, then JSON
                content = f.read()
                try:
                    return yaml.safe_load(content)
                except yaml.YAMLError:
                    return json.loads(content)
    
    @staticmethod
    def validate_cwl_workflow(cwl_data: Dict[str, Any]) -> bool:
        """Validate basic CWL workflow structure.
        
        Args:
            cwl_data: CWL workflow dictionary
            
        Returns:
            True if valid CWL workflow structure
            
        Raises:
            ValueError: If validation fails
        """
        if not isinstance(cwl_data, dict):
            raise ValueError("CWL workflow must be a dictionary")
        
        # Check for required CWL fields
        if 'class' not in cwl_data:
            raise ValueError("CWL workflow missing 'class' field")
        
        if cwl_data.get('class') != 'Workflow':
            raise ValueError(f"Expected CWL class 'Workflow', got '{cwl_data.get('class')}'")
        
        # Check for steps
        if 'steps' not in cwl_data:
            raise ValueError("CWL workflow missing 'steps' field")
        
        if not isinstance(cwl_data['steps'], dict):
            raise ValueError("CWL workflow 'steps' must be a dictionary")
        
        logger.info("CWL workflow structure validated successfully")
        return True

