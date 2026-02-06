"""
TaxonomicClassification service step validator.

This validator handles:
- Validating parameter types, values, and required fields for TaxonomicClassification
- Ensuring proper input source validation
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator, ValidationError
import logging

from .base_validator import BaseStepValidator, ValidationResult

logger = logging.getLogger(__name__)


class PairedEndLib(BaseModel):
    """Model for paired-end library structure."""
    read1: str
    read2: Optional[str] = None
    interleaved: bool = False
    read_orientation_outward: bool = False
    platform: Optional[str] = None
    
    class Config:
        extra = "allow"


class SingleEndLib(BaseModel):
    """Model for single-end library structure."""
    read: str
    platform: Optional[str] = None
    
    class Config:
        extra = "allow"


class SRRLib(BaseModel):
    """Model for SRR library structure (used in srr_libs)."""
    title: str
    srr_accession: str
    sample_id: str
    
    class Config:
        extra = "allow"


class TaxonomicClassificationParams(BaseModel):
    """
    Pydantic model for TaxonomicClassification step parameters.
    
    This model defines the expected structure, types, and validation rules
    for TaxonomicClassification service parameters.
    """
    # Required output parameters
    output_path: str = Field(..., description="Workspace output path for results")
    output_file: str = Field(..., description="Output file basename")
    
    # Required parameters with defaults
    host_genome: str = Field("no_host", description="Host genome for filtering")
    analysis_type: str = Field("16S", description="Analysis type: pathogen/microbiome/16S")
    database: str = Field("SILVA", description="Reference database: bvbrc/greengenes/silva/standard")
    
    # Optional input sources - at least one must be provided
    paired_end_libs: Optional[List[Dict[str, Any]]] = Field(None, description="List of paired-end libraries with sample IDs")
    single_end_libs: Optional[List[Dict[str, Any]]] = Field(None, description="List of single-end libraries with sample IDs")
    srr_libs: Optional[List[Dict[str, Any]]] = Field(None, description="List of SRA datasets with sample IDs")
    
    # Optional parameters
    save_classified_sequences: bool = Field(False, description="Save classified sequences")
    save_unclassified_sequences: bool = Field(False, description="Save unclassified sequences")
    sequence_type: Optional[str] = Field(None, description="Sequence type: 16S/18S/ITS/rRNA/DNA/RNA")
    confidence_interval: float = Field(0.1, description="Classification confidence threshold")
    
    # Allow additional fields (for flexibility with variable references)
    class Config:
        extra = "allow"
    
    @field_validator('analysis_type')
    @classmethod
    def validate_analysis_type(cls, v):
        """Validate analysis_type enum."""
        if v is not None:
            valid_types = ['pathogen', 'microbiome', '16S']
            if v not in valid_types:
                raise ValueError(f"analysis_type must be one of {valid_types}, got '{v}'")
        return v
    
    @field_validator('database')
    @classmethod
    def validate_database(cls, v):
        """Validate database enum."""
        if v is not None:
            valid_databases = ['bvbrc', 'Greengenes', 'SILVA', 'standard']
            if v not in valid_databases:
                raise ValueError(f"database must be one of {valid_databases}, got '{v}'")
        return v
    
    @field_validator('host_genome')
    @classmethod
    def validate_host_genome(cls, v):
        """Validate host_genome parameter."""
        if v is not None:
            if not isinstance(v, str):
                raise ValueError(f"host_genome must be a string, got {type(v)}")
            valid_hosts = [
                'homo_sapiens',
                'mus_musculus',
                'rattus_norvegicus',
                'caenorhabditis_elegans',
                'drosophila_melanogaster_strain',
                'danio_rerio_strain_tuebingen',
                'gallus_gallus',
                'macaca_mulatta',
                'mustela_putorius_furo',
                'sus_scrofa',
                'no_host'
            ]
            if v not in valid_hosts:
                raise ValueError(f"host_genome must be one of {valid_hosts}, got '{v}'")
        return v
    
    @field_validator('save_classified_sequences', 'save_unclassified_sequences')
    @classmethod
    def validate_boolean_flags(cls, v):
        """Validate boolean flag parameters."""
        if v is not None:
            if not isinstance(v, bool):
                # Try to convert string booleans
                if isinstance(v, str):
                    v_lower = v.lower()
                    if v_lower == 'true':
                        return True
                    elif v_lower == 'false':
                        return False
                raise ValueError(f"Parameter must be a boolean, got {type(v)}: {v}")
        return bool(v) if v is not None else v

    @field_validator('confidence_interval', mode='before')
    @classmethod
    def coerce_confidence_interval(cls, v):
        """Coerce confidence_interval from strings to float."""
        if v is None:
            return v
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            try:
                return float(v.strip())
            except ValueError:
                raise ValueError(f"confidence_interval must be a number, got '{v}'")
        raise ValueError(f"confidence_interval must be a number, got {type(v)}")
    
    @field_validator('sequence_type')
    @classmethod
    def validate_sequence_type(cls, v):
        """Validate sequence_type enum."""
        if v is not None:
            valid_types = ['wgs']
            v_str = str(v).strip()
            if v_str not in valid_types:
                raise ValueError(f"sequence_type must be one of {valid_types}, got '{v}'")
        return v
    
    @field_validator('confidence_interval')
    @classmethod
    def validate_confidence_interval(cls, v):
        """Validate confidence_interval range and convert to string."""
        if v is not None:
            # If it's a number, validate range and convert to string
            if isinstance(v, (int, float)):
                if v < 0.0 or v > 1.0:
                    raise ValueError(f"confidence_interval must be between 0.0 and 1.0, got {v}")
                return str(v)
            # If it's a string, validate it can be parsed as a float in range
            elif isinstance(v, str):
                try:
                    v_float = float(v)
                    if v_float < 0.0 or v_float > 1.0:
                        raise ValueError(f"confidence_interval must be between 0.0 and 1.0, got {v}")
                    return str(v_float)
                except ValueError:
                    raise ValueError(f"confidence_interval must be a valid number between 0.0 and 1.0, got '{v}'")
            else:
                raise ValueError(f"confidence_interval must be a number or string, got {type(v)}: {v}")
        return str(v) if v is not None else "0.1"
    
    @field_validator('output_path')
    @classmethod
    def validate_output_path(cls, v):
        """Validate output_path parameter."""
        if v is not None:
            if not isinstance(v, str):
                raise ValueError(f"output_path must be a string, got {type(v)}")
            v_str = str(v).strip()
            if not v_str:
                raise ValueError("output_path cannot be empty")
        return v
    
    @field_validator('output_file')
    @classmethod
    def validate_output_file(cls, v):
        """Validate output_file parameter."""
        if v is not None:
            if not isinstance(v, str):
                raise ValueError(f"output_file must be a string, got {type(v)}")
            v_str = str(v).strip()
            if not v_str:
                raise ValueError("output_file cannot be empty")
        return v
    
    @field_validator('paired_end_libs', 'single_end_libs', 'srr_libs')
    @classmethod
    def validate_lib_lists(cls, v):
        """Validate library lists if provided."""
        if v is not None:
            if not isinstance(v, list):
                raise ValueError("Library list must be a list")
            if len(v) == 0:
                raise ValueError("Library list cannot be empty")
        return v


class TaxonomicClassificationValidator(BaseStepValidator):
    """
    Validator for TaxonomicClassification service steps.
    
    Validates step structure and parameters for TaxonomicClassification.
    Ensures that at least one input source is provided.
    """
    
    def validate_params(
        self,
        params: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        """
        Validate TaxonomicClassification step parameters using Pydantic model.
        
        Args:
            params: Step parameters dictionary
            app_name: Application name
        
        Returns:
            ValidationResult with validated params, warnings, and errors
        """
        warnings = []
        errors = []
        validated_params = params.copy()
        
        # Check that at least one input source is provided
        has_input = (
            params.get('paired_end_libs') or 
            params.get('single_end_libs') or 
            params.get('srr_libs')
        )
        if not has_input:
            errors.append(
                "At least one input source must be provided: "
                "'paired_end_libs', 'single_end_libs', or 'srr_libs'"
            )
        
        # Check for multiple input sources (warn, not error)
        input_count = sum([
            1 if params.get('paired_end_libs') else 0,
            1 if params.get('single_end_libs') else 0,
            1 if params.get('srr_libs') else 0
        ])
        if input_count > 1:
            warnings.append(
                "Multiple input sources provided. "
                "Only one input source should be specified."
            )
        
        try:
            # Validate using Pydantic model
            validated_model = TaxonomicClassificationParams(**params)
            validated_params = validated_model.model_dump(exclude_none=False)
            
            # Additional business logic validations
            # Validate paired_end_libs structure if provided
            if validated_params.get('paired_end_libs'):
                for i, lib in enumerate(validated_params['paired_end_libs']):
                    try:
                        PairedEndLib(**lib)
                    except ValidationError as e:
                        errors.append(
                            f"paired_end_libs[{i}]: Invalid structure - {e.errors()[0]['msg']}"
                        )
            
            # Validate single_end_libs structure if provided
            if validated_params.get('single_end_libs'):
                for i, lib in enumerate(validated_params['single_end_libs']):
                    try:
                        SingleEndLib(**lib)
                    except ValidationError as e:
                        errors.append(
                            f"single_end_libs[{i}]: Invalid structure - {e.errors()[0]['msg']}"
                        )
            
            # Validate srr_libs structure if provided
            if validated_params.get('srr_libs'):
                for i, lib in enumerate(validated_params['srr_libs']):
                    try:
                        SRRLib(**lib)
                    except ValidationError as e:
                        errors.append(
                            f"srr_libs[{i}]: Invalid structure - {e.errors()[0]['msg']}. "
                            "Expected fields: 'title', 'srr_accession', 'sample_id'"
                        )
            
            # Validate output_path format
            output_path = validated_params.get('output_path', '')
            if output_path and not output_path.startswith('${') and not output_path.startswith('/'):
                warnings.append(
                    f"output_path '{output_path}' doesn't appear to be a valid path "
                    "(should start with '/' or be a variable reference)"
                )
            
            # Validate output_file format
            output_file = validated_params.get('output_file', '')
            if output_file:
                # Check for invalid characters in filename
                invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
                if any(char in output_file for char in invalid_chars):
                    warnings.append(
                        f"output_file '{output_file}' contains invalid characters. "
                        "Filenames should not contain: / \\ : * ? \" < > |"
                    )
            
            # Ensure all required parameters are present
            required_params = ['host_genome', 'analysis_type', 'database', 'output_path', 'output_file']
            missing_params = [p for p in required_params if p not in validated_params]
            if missing_params:
                errors.append(
                    f"Missing required parameters: {', '.join(missing_params)}"
                )
            
            logger.debug(
                f"TaxonomicClassification: Validated {len(validated_params)} parameters"
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
                f"TaxonomicClassification: Validation failed with {len(errors)} error(s)"
            )
        except Exception as e:
            # Unexpected errors during validation
            errors.append(f"Unexpected validation error: {str(e)}")
            logger.error(
                f"TaxonomicClassification: Unexpected validation error: {e}",
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
        Validate TaxonomicClassification step outputs.
        
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
        
        # TaxonomicClassification-specific output validation
        # Common output keys for TaxonomicClassification
        expected_outputs = ['classification_report']
        
        for key in outputs.keys():
            # Check if output key is recognized
            if key not in expected_outputs:
                warnings.append(
                    f"Output key '{key}' is not a standard TaxonomicClassification output. "
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

