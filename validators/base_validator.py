"""
Base validator interface for service-specific step validators.

This module provides the abstract base class that all service-specific
validators must implement.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """
    Result of validating a step.
    
    Attributes:
        params: Validated parameters dictionary
        warnings: List of non-critical validation warnings
        errors: List of critical validation errors (if non-empty, should raise exception)
        status: Validation status - "success" if no errors, "failure" if errors exist
    """
    params: Dict[str, Any]
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    @property
    def status(self) -> str:
        """Get validation status: 'success' if no errors, 'failure' if errors exist."""
        return "success" if len(self.errors) == 0 else "failure"
    
    def has_errors(self) -> bool:
        """Check if validation result contains errors."""
        return len(self.errors) > 0
    
    def has_warnings(self) -> bool:
        """Check if validation result contains warnings."""
        return len(self.warnings) > 0


class BaseStepValidator(ABC):
    """
    Base validator interface for service-specific step validation.
    
    Each service should implement a validator that:
    1. Validates parameter types, values, and required fields
    2. Validates step structure (app, params, outputs)
    """
    
    def __init__(self):
        """Initialize the validator."""
        self.service_name = self.__class__.__name__.replace("Validator", "")
    
    @abstractmethod
    def validate_params(
        self,
        params: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        """
        Validate step parameters.
        
        This method should:
        - Check parameter types
        - Validate parameter values
        - Check required fields are present
        - Validate parameter combinations
        
        Args:
            params: Step parameters dictionary
            app_name: Application/service name
        
        Returns:
            ValidationResult with validated params, warnings, and errors
        """
        pass
    
    def validate_step(
        self,
        step: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        """
        Validate entire step structure (app, params, outputs).
        
        This is the main entry point that validates the complete step.
        It calls validate_params() and also validates outputs if present.
        
        Args:
            step: Complete step dictionary (with app, params, outputs, etc.)
            app_name: Application/service name
        
        Returns:
            ValidationResult with validated params, warnings, and errors
        """
        errors = []
        warnings = []
        params = step.get('params', {})
        
        # Validate app name matches
        step_app = step.get('app', '')
        if step_app != app_name:
            errors.append(
                f"Step app '{step_app}' does not match validator app '{app_name}'"
            )
        
        # Validate params
        param_result = self.validate_params(params, app_name)
        params = param_result.params
        errors.extend(param_result.errors)
        warnings.extend(param_result.warnings)
        
        # Validate outputs structure if present
        if 'outputs' in step and step['outputs']:
            output_result = self.validate_outputs(step['outputs'], params, app_name)
            warnings.extend(output_result.warnings)
            errors.extend(output_result.errors)
        
        return ValidationResult(
            params=params,
            warnings=warnings,
            errors=errors
        )
    
    def validate_outputs(
        self,
        outputs: Dict[str, str],
        params: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        """
        Validate step outputs structure.
        
        Override this method to add service-specific output validation.
        By default, just checks that outputs is a dictionary.
        
        Args:
            outputs: Step outputs dictionary
            params: Step parameters (for context)
            app_name: Application/service name
        
        Returns:
            ValidationResult with warnings and errors
        """
        warnings = []
        errors = []
        
        if not isinstance(outputs, dict):
            errors.append(f"Step outputs must be a dictionary, got {type(outputs)}")
        else:
            # Basic validation: check that output values are strings
            for key, value in outputs.items():
                if not isinstance(value, str):
                    warnings.append(
                        f"Output '{key}' has non-string value: {type(value)}"
                    )
        
        return ValidationResult(
            params={},  # Not used for output validation
            warnings=warnings,
            errors=errors
        )
