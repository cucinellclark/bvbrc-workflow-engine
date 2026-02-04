"""
GenomeAnnotation service default parameter provider.

This module provides default parameters for GenomeAnnotation service steps.
"""
from typing import Dict, Any
import logging

from .base_defaults import BaseDefaults

logger = logging.getLogger(__name__)


class GenomeAnnotationDefaults(BaseDefaults):
    """
    Default parameter provider for GenomeAnnotation service steps.
    
    Provides sensible default values for optional GenomeAnnotation parameters.
    """
    
    def get_default_params(self, app_name: str) -> Dict[str, Any]:
        """
        Get default parameters for GenomeAnnotation.
        
        Args:
            app_name: Application name (should be "GenomeAnnotation")
        
        Returns:
            Dictionary of default parameter values
        """
        return {
            "output_file": "annotation_output",
        }

