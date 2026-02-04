"""
Defaults registry for managing service-specific default parameter providers.

This module provides a registry system that makes it easy to add new defaults
providers for different service types.
"""
import logging
from typing import Dict, Optional, Type
from .base_defaults import BaseDefaults

logger = logging.getLogger(__name__)


class DefaultsRegistry:
    """
    Registry for service-specific default parameter providers.
    
    Allows easy registration and retrieval of defaults providers for different service types.
    """
    
    def __init__(self):
        """Initialize the defaults registry."""
        self._defaults: Dict[str, Type[BaseDefaults]] = {}
    
    def register(
        self,
        app_name: str,
        defaults_class: Type[BaseDefaults]
    ) -> None:
        """
        Register a defaults provider for an app/service.
        
        Args:
            app_name: Application/service name (e.g., "GenomeAnnotation")
            defaults_class: Defaults class (must inherit from BaseDefaults)
        
        Raises:
            ValueError: If defaults_class doesn't inherit from BaseDefaults
        """
        if not issubclass(defaults_class, BaseDefaults):
            raise ValueError(
                f"Defaults class {defaults_class.__name__} must inherit from BaseDefaults"
            )
        
        self._defaults[app_name] = defaults_class
        logger.info(f"Registered defaults provider for app: {app_name}")
    
    def get(self, app_name: str) -> Optional[BaseDefaults]:
        """
        Get a defaults provider instance for an app/service.
        
        Args:
            app_name: Application/service name
        
        Returns:
            Defaults provider instance or None if not found
        """
        defaults_class = self._defaults.get(app_name)
        if defaults_class:
            return defaults_class()
        return None
    
    def is_registered(self, app_name: str) -> bool:
        """
        Check if a defaults provider is registered for an app.
        
        Args:
            app_name: Application/service name
        
        Returns:
            True if defaults provider is registered, False otherwise
        """
        return app_name in self._defaults
    
    def list_registered(self) -> list:
        """
        List all registered app names.
        
        Returns:
            List of app names that have defaults providers registered
        """
        return list(self._defaults.keys())


# Global defaults registry instance
_global_defaults_registry = DefaultsRegistry()


def register_defaults(
    app_name: str,
    defaults_class: Type[BaseDefaults]
) -> None:
    """
    Register a defaults provider in the global registry.
    
    Convenience function for registering defaults providers.
    
    Args:
        app_name: Application/service name (e.g., "GenomeAnnotation")
        defaults_class: Defaults class (must inherit from BaseDefaults)
    
    Example:
        >>> from workflow_engine.validators import BaseDefaults, register_defaults
        >>> 
        >>> class MyServiceDefaults(BaseDefaults):
        >>>     def get_default_params(self, app_name):
        >>>         return {"output_file": "default_output"}
        >>> 
        >>> register_defaults("MyService", MyServiceDefaults)
    """
    _global_defaults_registry.register(app_name, defaults_class)


def get_defaults(app_name: str) -> Optional[BaseDefaults]:
    """
    Get a defaults provider instance from the global registry.
    
    Args:
        app_name: Application/service name
    
    Returns:
        Defaults provider instance or None if not found
    """
    return _global_defaults_registry.get(app_name)


def get_defaults_registry() -> DefaultsRegistry:
    """
    Get the global defaults registry instance.
    
    Returns:
        The global DefaultsRegistry instance
    """
    return _global_defaults_registry

