"""
Template for creating new service-specific default parameter providers.

Copy this file and modify it to create a defaults provider for a new service.
"""
from typing import Dict, Any
import logging

from .base_defaults import BaseDefaults

logger = logging.getLogger(__name__)


class ServiceNameDefaults(BaseDefaults):
    """
    Default parameter provider for {ServiceName} service steps.
    
    Replace {ServiceName} with your actual service name.
    Provides sensible default values for optional {ServiceName} parameters.
    """
    
    def get_default_params(self, app_name: str) -> Dict[str, Any]:
        """
        Get default parameters for {ServiceName}.
        
        Args:
            app_name: Application name
        
        Returns:
            Dictionary of default parameter values
        """
        return {
            "optional_param": "default_value",
            # Add more defaults as needed
        }


# To register this defaults provider, add to validators/__init__.py:
# from .service_name_defaults import ServiceNameDefaults
# register_defaults("ServiceName", ServiceNameDefaults)

