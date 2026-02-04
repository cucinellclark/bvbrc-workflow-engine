"""
Template for creating new service-specific step validators.

Copy this file and modify it to create a validator for a new service.
Note: Defaults should be implemented separately in a defaults provider.
"""
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator, ValidationError
import logging

from .base_validator import BaseStepValidator, ValidationResult

logger = logging.getLogger(__name__)


class ServiceNameParams(BaseModel):
    """
    Pydantic model for {ServiceName} step parameters.
    
    Define the expected structure, types, and validation rules here.
    Replace {ServiceName} with your actual service name.
    """
    # Required parameters
    required_param: str = Field(..., description="Description of required parameter")
    
    # Optional parameters with defaults
    optional_param: str = Field("default_value", description="Description of optional parameter")
    another_param: Optional[int] = Field(None, description="Description of another parameter")
    
    # Allow additional fields (for flexibility with variable references)
    class Config:
        extra = "allow"
    
    @field_validator('another_param')
    @classmethod
    def validate_another_param(cls, v):
        """Add custom validation logic here."""
        if v is not None and v <= 0:
            raise ValueError("another_param must be a positive integer if provided")
        return v


class ServiceNameValidator(BaseStepValidator):
    """
    Validator for {ServiceName} service steps.
    
    Replace {ServiceName} with your actual service name.
    Note: Defaults should be implemented in a separate ServiceNameDefaults class.
    """
    
    def validate_params(
        self,
        params: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        """
        Validate {ServiceName} step parameters using Pydantic model.
        
        Args:
            params: Step parameters dictionary
            app_name: Application name
        
        Returns:
            ValidationResult with validated params, warnings, and errors
        """
        warnings = []
        errors = []
        validated_params = params.copy()
        
        try:
            # Validate using Pydantic model
            validated_model = ServiceNameParams(**params)
            validated_params = validated_model.model_dump(exclude_none=False)
            
            # Additional business logic validations
            # Add custom validation logic here
            
            logger.debug(
                f"{app_name}: Validated {len(validated_params)} parameters"
            )
            
        except ValidationError as e:
            # Pydantic validation errors
            for error in e.errors():
                field = '.'.join(str(loc) for loc in error['loc'])
                error_msg = error['msg']
                errors.append(
                    f"Parameter '{field}': {error_msg}"
                )
            logger.warning(
                f"{app_name}: Validation failed with {len(errors)} error(s)"
            )
        except Exception as e:
            # Unexpected errors during validation
            errors.append(f"Unexpected validation error: {str(e)}")
            logger.error(
                f"{app_name}: Unexpected validation error: {e}",
                exc_info=True
            )
        
        return ValidationResult(
            params=validated_params,
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
        Validate {ServiceName} step outputs.
        
        Override this method to add service-specific output validation.
        
        Args:
            outputs: Step outputs dictionary
            params: Step parameters (for context)
            app_name: Application name
        
        Returns:
            ValidationResult with warnings and errors
        """
        warnings = []
        errors = []
        
        # Call parent validation first
        parent_result = super().validate_outputs(outputs, params, app_name)
        warnings.extend(parent_result.warnings)
        errors.extend(parent_result.errors)
        
        # Add service-specific output validation here
        
        return ValidationResult(
            params={},
            warnings=warnings,
            errors=errors
        )


# To register this validator, add to validators/__init__.py:
# from .service_name_validator import ServiceNameValidator
# register_validator("ServiceName", ServiceNameValidator)

