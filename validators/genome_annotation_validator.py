"""
GenomeAnnotation service step validator.

This validator handles:
- Adding default parameters for GenomeAnnotation steps
- Validating parameter types, values, and required fields
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator, ValidationError
import logging

from .base_validator import BaseStepValidator, ValidationResult

logger = logging.getLogger(__name__)


class GenomeAnnotationParams(BaseModel):
    """
    Pydantic model for GenomeAnnotation step parameters.
    
    This model defines the expected structure, types, and validation rules
    for GenomeAnnotation service parameters.
    """
    # Required parameters
    contigs: str = Field(..., description="Path to contigs file (FASTA format)")
    output_path: str = Field(..., description="Workspace output path for annotation results")
    
    # Optional parameters with defaults
    scientific_name: Optional[str] = Field(None, description="Scientific name of the organism")
    taxonomy_id: Optional[str] = Field(None, description="NCBI taxonomy ID")
    output_file: str = Field("annotation_output", description="Output file name")
    recipe: Optional[str] = Field(None, description="Annotation recipe to use")
    
    # Allow additional fields (for flexibility with variable references)
    class Config:
        extra = "allow"
    
    @field_validator('taxonomy_id')
    @classmethod
    def validate_taxonomy_id(cls, v):
        """Validate taxonomy_id if provided."""
        if v is not None:
            # Convert to string if it's a number (from JSON)
            v_str = str(v).strip()
            # Check if it's a valid integer string
            try:
                taxonomy_int = int(v_str)
            except ValueError:
                raise ValueError(f"taxonomy_id must be a valid integer, got '{v}'")
            # Check if it's positive
            if taxonomy_int <= 0:
                raise ValueError(f"taxonomy_id must be a positive integer, got '{v}'")
            # Return as string (to preserve original format, but validated)
            return v_str
        return v
    
    @field_validator('contigs')
    @classmethod
    def validate_contigs(cls, v):
        """Validate contigs parameter."""
        if not v or not isinstance(v, str):
            raise ValueError("contigs must be a non-empty string")
        return v
    
    @field_validator('output_path')
    @classmethod
    def validate_output_path(cls, v):
        """Validate output_path parameter."""
        if not v or not isinstance(v, str):
            raise ValueError("output_path must be a non-empty string")
        return v


class GenomeAnnotationValidator(BaseStepValidator):
    """
    Validator for GenomeAnnotation service steps.
    
    Validates step structure and parameters for GenomeAnnotation.
    """
    
    def validate_params(
        self,
        params: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        """
        Validate GenomeAnnotation step parameters using Pydantic model.
        
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
            # Convert params dict to model instance (this validates types and values)
            validated_model = GenomeAnnotationParams(**params)
            
            # Convert back to dict (Pydantic handles type coercion)
            validated_params = validated_model.model_dump(exclude_none=False)
            
            # Additional business logic validations
            # Check that either scientific_name or taxonomy_id is provided (recommended)
            if not validated_params.get('scientific_name') and not validated_params.get('taxonomy_id'):
                warnings.append(
                    "Neither 'scientific_name' nor 'taxonomy_id' is provided. "
                    "At least one is recommended for proper annotation."
                )
            
            # Check that contigs path looks reasonable (basic sanity check)
            contigs = validated_params.get('contigs', '')
            if contigs and not contigs.startswith('${') and not contigs.endswith(('.fasta', '.fa', '.fna')):
                # Not a variable reference and doesn't end with common FASTA extensions
                warnings.append(
                    f"contigs path '{contigs}' doesn't appear to be a FASTA file "
                    "(should end with .fasta, .fa, or .fna) or a variable reference"
                )
            
            logger.debug(
                f"GenomeAnnotation: Validated {len(validated_params)} parameters"
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
                f"GenomeAnnotation: Validation failed with {len(errors)} error(s)"
            )
        except Exception as e:
            # Unexpected errors during validation
            errors.append(f"Unexpected validation error: {str(e)}")
            logger.error(
                f"GenomeAnnotation: Unexpected validation error: {e}",
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
        Validate GenomeAnnotation step outputs.
        
        Checks that outputs reference valid parameters and have reasonable structure.
        
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
        
        # GenomeAnnotation-specific output validation
        # Common output keys for GenomeAnnotation
        expected_outputs = ['contigs_fasta', 'genome_file', 'annotation_file']
        
        for key in outputs.keys():
            # Check if output key is recognized
            if key not in expected_outputs:
                warnings.append(
                    f"Output key '{key}' is not a standard GenomeAnnotation output. "
                    f"Expected keys: {', '.join(expected_outputs)}"
                )
            
            # Check that output value references params appropriately
            output_value = outputs[key]
            if '${params.output_path}' not in output_value and '${params.output_file}' not in output_value:
                # Not a hard error, but worth warning about
                if not output_value.startswith('${'):
                    warnings.append(
                        f"Output '{key}' path doesn't reference params.output_path or params.output_file. "
                        "This may cause issues if output paths change."
                    )
        
        return ValidationResult(
            params={},
            warnings=warnings,
            errors=errors
        )

