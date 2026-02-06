"""
SimilarGenomeFinder service step validator.

This validator handles:
- Validating parameter types, values, and required fields for SimilarGenomeFinder
- Special handling for immediate-return service (doesn't submit a job, returns results directly)
"""
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator, ValidationError
import logging

from .base_validator import BaseStepValidator, ValidationResult

logger = logging.getLogger(__name__)


class SimilarGenomeFinderParams(BaseModel):
    """
    Pydantic model for SimilarGenomeFinder step parameters.
    
    This model defines the expected structure, types, and validation rules
    for SimilarGenomeFinder service parameters.
    
    Note: SimilarGenomeFinder is a special service that returns results immediately
    instead of submitting a job. It calls Minhash.compute_genome_distance_for_genome2
    or Minhash.compute_genome_distance_for_fasta2 directly.
    """
    # Required output parameters (may not be used for immediate-return service, but required by interface)
    output_path: str = Field(..., description="Workspace output path for results")
    output_file: str = Field(..., description="Output file basename")
    
    # Required query input - at least one must be provided
    selectedGenomeId: Optional[str] = Field(None, description="Reference genome ID")
    fasta_file: Optional[str] = Field(None, description="FASTA file input")
    
    # Optional parameters
    max_pvalue: Optional[float] = Field(None, description="Maximum p-value threshold")
    max_distance: Optional[float] = Field(None, description="Maximum distance threshold")
    max_hits: Optional[int] = Field(None, description="Maximum number of hits")
    include_reference: Optional[bool] = Field(None, description="Include reference genomes")
    include_representative: Optional[bool] = Field(None, description="Include representative genomes")
    include_bacterial: Optional[bool] = Field(None, description="Include bacterial genomes")
    include_viral: Optional[bool] = Field(None, description="Include viral genomes")
    
    # Allow additional fields (for flexibility with variable references)
    class Config:
        extra = "allow"
    
    @field_validator('max_pvalue', 'max_distance')
    @classmethod
    def validate_float_params(cls, v):
        """Validate float parameters."""
        if v is not None:
            if not isinstance(v, (int, float)) or v < 0:
                raise ValueError(f"Value must be a non-negative number, got {v}")
        return float(v) if v is not None else v
    
    @field_validator('max_hits')
    @classmethod
    def validate_max_hits(cls, v):
        """Validate max_hits parameter."""
        if v is not None:
            if not isinstance(v, int) or v <= 0:
                raise ValueError(f"max_hits must be a positive integer, got {v}")
        return v
    
    @field_validator('selectedGenomeId')
    @classmethod
    def validate_selected_genome_id(cls, v):
        """Validate selectedGenomeId format."""
        if v is not None:
            if not isinstance(v, str) or not v.strip():
                raise ValueError("selectedGenomeId must be a non-empty string")
        return v
    
    @field_validator('fasta_file')
    @classmethod
    def validate_fasta_file(cls, v):
        """Validate fasta_file path format."""
        if v is not None:
            if not isinstance(v, str) or not v.strip():
                raise ValueError("fasta_file must be a non-empty string")
            # Basic sanity check - should be a path or variable reference
            if not v.startswith('${') and not v.endswith(('.fasta', '.fa', '.fna', '.fas')):
                # Not a variable reference and doesn't end with common FASTA extensions
                # This is just a warning-level check, not an error
                pass  # We'll handle this in the validator class as a warning
        return v


class SimilarGenomeFinderValidator(BaseStepValidator):
    """
    Validator for SimilarGenomeFinder service steps.
    
    Validates step structure and parameters for SimilarGenomeFinder.
    
    Note: SimilarGenomeFinder is a special immediate-return service that doesn't
    submit a job. It returns results directly via Minhash API calls. The validator
    still validates parameters but should account for the fact that this service
    doesn't produce file outputs in the traditional sense.
    """
    
    def validate_params(
        self,
        params: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        """
        Validate SimilarGenomeFinder step parameters using Pydantic model.
        
        Args:
            params: Step parameters dictionary
            app_name: Application name
        
        Returns:
            ValidationResult with validated params, warnings, and errors
        """
        warnings = []
        errors = []
        validated_params = params.copy()
        
        # Check that at least one query input is provided
        has_query_input = (
            params.get('selectedGenomeId') or 
            params.get('fasta_file')
        )
        if not has_query_input:
            errors.append(
                "At least one query input must be provided: "
                "'selectedGenomeId' or 'fasta_file'"
            )
        
        # Check for multiple query inputs (warn, not error)
        query_count = sum([
            1 if params.get('selectedGenomeId') else 0,
            1 if params.get('fasta_file') else 0
        ])
        if query_count > 1:
            warnings.append(
                "Multiple query inputs provided (selectedGenomeId and fasta_file). "
                "Only one should be specified. selectedGenomeId will take precedence."
            )
        
        try:
            # Validate using Pydantic model
            validated_model = SimilarGenomeFinderParams(**params)
            validated_params = validated_model.model_dump(exclude_none=False)
            
            # Additional business logic validations
            # Validate fasta_file path format if provided
            fasta_file = validated_params.get('fasta_file')
            if fasta_file:
                if not fasta_file.startswith('${') and not fasta_file.endswith(('.fasta', '.fa', '.fna', '.fas')):
                    warnings.append(
                        f"fasta_file '{fasta_file}' doesn't appear to be a FASTA file "
                        "(should end with .fasta, .fa, .fna, or .fas) or be a variable reference"
                    )
            
            # Validate selectedGenomeId format if provided (basic check)
            selected_genome_id = validated_params.get('selectedGenomeId')
            if selected_genome_id:
                # Basic format check - genome IDs are typically alphanumeric with underscores/hyphens
                if not isinstance(selected_genome_id, str) or len(selected_genome_id.strip()) == 0:
                    errors.append("selectedGenomeId must be a non-empty string")
            
            # Note about immediate-return behavior
            warnings.append(
                "SimilarGenomeFinder is an immediate-return service that returns results "
                "directly instead of submitting a job. Results are returned via the API response."
            )
            
            # Validate output_path format (even though it may not be used)
            output_path = validated_params.get('output_path', '')
            if output_path and not output_path.startswith('${') and not output_path.startswith('/'):
                warnings.append(
                    f"output_path '{output_path}' doesn't appear to be a valid path "
                    "(should start with '/' or be a variable reference). "
                    "Note: output_path may not be used for immediate-return services."
                )
            
            logger.debug(
                f"SimilarGenomeFinder: Validated {len(validated_params)} parameters"
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
                f"SimilarGenomeFinder: Validation failed with {len(errors)} error(s)"
            )
        except Exception as e:
            # Unexpected errors during validation
            errors.append(f"Unexpected validation error: {str(e)}")
            logger.error(
                f"SimilarGenomeFinder: Unexpected validation error: {e}",
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
        Validate SimilarGenomeFinder step outputs.
        
        Note: SimilarGenomeFinder is an immediate-return service that returns results
        directly via API response, not as files. Outputs may be empty or contain
        references to where results should be stored.
        
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
        
        # SimilarGenomeFinder-specific output validation
        # This service returns results immediately, so outputs may be empty or minimal
        if not outputs:
            warnings.append(
                "SimilarGenomeFinder is an immediate-return service. "
                "Outputs are returned directly in the API response, not as files. "
                "Consider storing results in outputs if needed for workflow chaining."
            )
        else:
            # If outputs are provided, validate them
            for key in outputs.keys():
                output_value = outputs[key]
                # Check that output value is reasonable
                if not output_value.startswith('${') and not output_value.startswith('/'):
                    warnings.append(
                        f"Output '{key}' path doesn't appear to be a valid path "
                        "(should start with '/' or be a variable reference). "
                        "Note: SimilarGenomeFinder returns results directly, not as files."
                    )
        
        return ValidationResult(
            params={},
            warnings=warnings,
            errors=errors
        )

