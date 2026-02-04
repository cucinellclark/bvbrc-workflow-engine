"""
Validator registry for managing service-specific step validators.

This module provides a registry system that makes it easy to add new validators
for different service types.
"""
import logging
from typing import Dict, Optional, Type
from .base_validator import BaseStepValidator

logger = logging.getLogger(__name__)


class ValidatorRegistry:
    """
    Registry for service-specific step validators.
    
    Allows easy registration and retrieval of validators for different service types.
    """
    
    def __init__(self):
        """Initialize the validator registry."""
        self._validators: Dict[str, Type[BaseStepValidator]] = {}
    
    def register(
        self,
        app_name: str,
        validator_class: Type[BaseStepValidator]
    ) -> None:
        """
        Register a validator for an app/service.
        
        Args:
            app_name: Application/service name (e.g., "GenomeAnnotation")
            validator_class: Validator class (must inherit from BaseStepValidator)
        
        Raises:
            ValueError: If validator_class doesn't inherit from BaseStepValidator
        """
        if not issubclass(validator_class, BaseStepValidator):
            raise ValueError(
                f"Validator class {validator_class.__name__} must inherit from BaseStepValidator"
            )
        
        self._validators[app_name] = validator_class
        logger.info(f"Registered validator for app: {app_name}")
    
    def get(self, app_name: str) -> Optional[BaseStepValidator]:
        """
        Get a validator instance for an app/service.
        
        Args:
            app_name: Application/service name
        
        Returns:
            Validator instance or None if not found
        """
        validator_class = self._validators.get(app_name)
        if validator_class:
            return validator_class()
        return None
    
    def is_registered(self, app_name: str) -> bool:
        """
        Check if a validator is registered for an app.
        
        Args:
            app_name: Application/service name
        
        Returns:
            True if validator is registered, False otherwise
        """
        return app_name in self._validators
    
    def list_registered(self) -> list:
        """
        List all registered app names.
        
        Returns:
            List of app names that have validators registered
        """
        return list(self._validators.keys())


# Global validator registry instance
_global_registry = ValidatorRegistry()


def register_validator(
    app_name: str,
    validator_class: Type[BaseStepValidator]
) -> None:
    """
    Register a validator in the global registry.
    
    Convenience function for registering validators.
    
    Args:
        app_name: Application/service name (e.g., "GenomeAnnotation")
        validator_class: Validator class (must inherit from BaseStepValidator)
    
    Example:
        >>> from workflow_engine.validators import BaseStepValidator, register_validator
        >>> 
        >>> class MyServiceValidator(BaseStepValidator):
        >>>     def get_default_params(self, app_name):
        >>>         return {"output_file": "default_output"}
        >>>     
        >>>     def validate_params(self, params, app_name):
        >>>         # Validation logic here
        >>>         return ValidationResult(params=params)
        >>> 
        >>> register_validator("MyService", MyServiceValidator)
    """
    _global_registry.register(app_name, validator_class)


def get_validator(app_name: str) -> Optional[BaseStepValidator]:
    """
    Get a validator instance from the global registry.
    
    Args:
        app_name: Application/service name
    
    Returns:
        Validator instance or None if not found
    """
    return _global_registry.get(app_name)


def get_registry() -> ValidatorRegistry:
    """
    Get the global validator registry instance.
    
    Returns:
        The global ValidatorRegistry instance
    """
    return _global_registry

