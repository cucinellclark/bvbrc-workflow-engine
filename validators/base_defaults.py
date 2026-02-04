"""
Base defaults interface for service-specific default parameter providers.

This module provides the abstract base class for adding default parameters
to workflow steps.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class BaseDefaults(ABC):
    """
    Base interface for service-specific default parameter providers.
    
    Each service should implement a defaults provider that:
    1. Defines default parameters for the service
    2. Provides a method to merge defaults with existing params (non-destructive)
    """
    
    def __init__(self):
        """Initialize the defaults provider."""
        self.service_name = self.__class__.__name__.replace("Defaults", "")
    
    @abstractmethod
    def get_default_params(self, app_name: str) -> Dict[str, Any]:
        """
        Get default parameters for a service.
        
        Args:
            app_name: Application/service name (e.g., "GenomeAnnotation")
        
        Returns:
            Dictionary of default parameter values
        """
        pass
    
    def apply_defaults(
        self,
        params: Dict[str, Any],
        app_name: str
    ) -> Dict[str, Any]:
        """
        Apply default parameters to existing parameters (non-destructive).
        
        Only adds missing keys, never overrides existing values.
        Performs deep merge for nested dictionaries.
        
        Args:
            params: Existing parameters dictionary
            app_name: Application/service name
        
        Returns:
            Parameters dictionary with defaults merged in
        """
        defaults = self.get_default_params(app_name)
        return self._merge_defaults(params, defaults, app_name)
    
    def _merge_defaults(
        self,
        existing: Dict[str, Any],
        defaults: Dict[str, Any],
        app_name: str
    ) -> Dict[str, Any]:
        """
        Merge default parameters with existing parameters (non-destructive).
        
        Only adds missing keys, never overrides existing values.
        Performs deep merge for nested dictionaries.
        
        Args:
            existing: Existing parameters dictionary
            defaults: Default parameters dictionary
            app_name: Application/service name (for logging)
        
        Returns:
            Merged parameters dictionary
        """
        merged = existing.copy()
        added_defaults = []
        
        for key, default_value in defaults.items():
            if key not in merged:
                # Key is missing, add default
                merged[key] = default_value
                added_defaults.append(key)
            elif isinstance(default_value, dict) and isinstance(merged[key], dict):
                # Both are dicts, perform deep merge
                merged[key] = self._merge_defaults(
                    merged[key],
                    default_value,
                    app_name
                )
        
        if added_defaults:
            logger.info(
                f"{app_name}: Applied default parameters: {', '.join(added_defaults)}"
            )
        
        return merged

