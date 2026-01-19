"""Tool mapper for converting CWL tool references to app names."""
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)


class ToolMapper:
    """Maps CWL tools to application names."""
    
    def __init__(self, mappings_file: Optional[Path] = None):
        """Initialize tool mapper.
        
        Args:
            mappings_file: Path to tool mappings YAML file. If None, uses default location.
        """
        if mappings_file is None:
            # Default to config/tool_mappings.yaml in cwl directory
            cwl_dir = Path(__file__).parent
            mappings_file = cwl_dir / "config" / "tool_mappings.yaml"
        
        self.mappings_file = Path(mappings_file)
        self.tool_mappings: Dict[str, str] = {}
        self._load_mappings()
    
    def _load_mappings(self):
        """Load tool mappings from configuration file."""
        if not self.mappings_file.exists():
            logger.warning(
                f"Tool mappings file not found: {self.mappings_file}. "
                "Using empty mappings. Create the file to map CWL tools to app names."
            )
            self.tool_mappings = {}
            return
        
        try:
            with open(self.mappings_file, 'r') as f:
                config = yaml.safe_load(f) or {}
                self.tool_mappings = config.get('tool_mappings', {})
            
            logger.info(
                f"Loaded {len(self.tool_mappings)} tool mappings from {self.mappings_file}"
            )
        except Exception as e:
            logger.error(f"Failed to load tool mappings: {e}")
            self.tool_mappings = {}
    
    def map_tool_to_app(self, tool_ref: str) -> str:
        """Map CWL tool reference to application name.
        
        Args:
            tool_ref: CWL tool reference (filename, path, or ID)
            
        Returns:
            Application name
            
        Raises:
            ValueError: If tool mapping not found
        """
        # Try exact match first
        if tool_ref in self.tool_mappings:
            app_name = self.tool_mappings[tool_ref]
            logger.debug(f"Mapped tool '{tool_ref}' to app '{app_name}'")
            return app_name
        
        # Try with just filename (without path)
        tool_filename = Path(tool_ref).name
        if tool_filename in self.tool_mappings:
            app_name = self.tool_mappings[tool_filename]
            logger.debug(f"Mapped tool '{tool_ref}' (filename: '{tool_filename}') to app '{app_name}'")
            return app_name
        
        # Try without .cwl extension
        tool_base = tool_filename.replace('.cwl', '')
        if tool_base in self.tool_mappings:
            app_name = self.tool_mappings[tool_base]
            logger.debug(f"Mapped tool '{tool_ref}' (base: '{tool_base}') to app '{app_name}'")
            return app_name
        
        # Try convention-based mapping (convert kebab-case to PascalCase)
        app_name = ToolMapper._convert_to_app_name(tool_base)
        logger.warning(
            f"No explicit mapping found for tool '{tool_ref}'. "
            f"Using convention-based name: '{app_name}'. "
            f"Consider adding explicit mapping in {self.mappings_file}"
        )
        return app_name
    
    @staticmethod
    def _convert_to_app_name(tool_name: str) -> str:
        """Convert tool name to app name using convention.
        
        Converts kebab-case or snake_case to PascalCase.
        
        Args:
            tool_name: Tool name (e.g., 'metagenome-binning', 'genome_annotation')
            
        Returns:
            App name in PascalCase (e.g., 'MetagenomeBinning', 'GenomeAnnotation')
        """
        # Replace hyphens and underscores with spaces, then title case
        parts = tool_name.replace('-', '_').split('_')
        app_name = ''.join(word.capitalize() for word in parts)
        return app_name
    
    def add_mapping(self, tool_ref: str, app_name: str):
        """Add a tool mapping.
        
        Args:
            tool_ref: CWL tool reference
            app_name: Application name
        """
        self.tool_mappings[tool_ref] = app_name
        logger.debug(f"Added mapping: '{tool_ref}' -> '{app_name}'")

