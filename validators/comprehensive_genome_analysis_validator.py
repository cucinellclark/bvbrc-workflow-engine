"""
ComprehensiveGenomeAnalysis service step validator.

This validator handles:
- Validating parameter types, values, and required fields for ComprehensiveGenomeAnalysis
- Ensuring default parameters are present
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


class ComprehensiveGenomeAnalysisParams(BaseModel):
    """
    Pydantic model for ComprehensiveGenomeAnalysis step parameters.
    
    This model defines the expected structure, types, and validation rules
    for ComprehensiveGenomeAnalysis service parameters.
    
    ComprehensiveGenomeAnalysis combines assembly and annotation into one service.
    """
    # Input sources - at least one must be provided
    srr_ids: Optional[List[str]] = Field(None, description="List of SRR IDs")
    paired_end_libs: Optional[List[Dict[str, Any]]] = Field(None, description="List of paired-end libraries")
    single_end_libs: Optional[List[Dict[str, Any]]] = Field(None, description="List of single-end libraries")
    
    # Required output parameters
    output_path: str = Field(..., description="Workspace output path for results")
    output_file: str = Field("cga_output", description="Output file name")
    
    # Optional recipe/strategy parameters
    recipe: Optional[str] = Field("auto", description="Assembly recipe to use")
    
    # Annotation parameters (optional)
    scientific_name: Optional[str] = Field(None, description="Scientific name of the organism")
    taxonomy_id: Optional[str] = Field(None, description="NCBI taxonomy ID")
    
    # Assembly parameters with defaults (should be provided by defaults provider)
    genome_size: int = Field(5000000, description="Expected genome size in base pairs")
    normalize: bool = Field(True, description="Whether to normalize reads")
    trim: bool = Field(True, description="Whether to trim reads")
    coverage: int = Field(200, description="Target coverage depth")
    expected_genome_size: int = Field(5, description="Expected genome size")
    genome_size_units: str = Field("M", description="Units for expected genome size")
    racon_iter: int = Field(2, description="Number of Racon iterations")
    pilon_iter: int = Field(2, description="Number of Pilon iterations")
    min_contig_len: int = Field(300, description="Minimum contig length")
    min_contig_cov: int = Field(5, description="Minimum contig coverage")
    filtlong: bool = Field(True, description="Whether to use Filtlong")
    target_depth: int = Field(200, description="Target sequencing depth")
    
    # Allow additional fields (for flexibility with variable references)
    class Config:
        extra = "allow"
    
    @field_validator('srr_ids')
    @classmethod
    def validate_srr_ids(cls, v):
        """Validate srr_ids if provided."""
        if v is not None:
            if not isinstance(v, list):
                raise ValueError("srr_ids must be a list")
            if len(v) == 0:
                raise ValueError("srr_ids cannot be empty")
            for srr_id in v:
                if not isinstance(srr_id, str) or not srr_id.strip():
                    raise ValueError(f"Invalid SRR ID: {srr_id}")
        return v
    
    @field_validator('paired_end_libs', 'single_end_libs')
    @classmethod
    def validate_lib_lists(cls, v):
        """Validate library lists if provided."""
        if v is not None:
            if not isinstance(v, list):
                raise ValueError("Library list must be a list")
            if len(v) == 0:
                raise ValueError("Library list cannot be empty")
        return v
    
    @field_validator('taxonomy_id')
    @classmethod
    def validate_taxonomy_id(cls, v):
        """Validate taxonomy_id if provided."""
        if v is not None:
            v_str = str(v).strip()
            try:
                taxonomy_int = int(v_str)
            except ValueError:
                raise ValueError(f"taxonomy_id must be a valid integer, got '{v}'")
            if taxonomy_int <= 0:
                raise ValueError(f"taxonomy_id must be a positive integer, got '{v}'")
            return v_str
        return v
    
    @field_validator('genome_size')
    @classmethod
    def validate_genome_size(cls, v):
        """Validate genome_size."""
        if v is not None:
            if not isinstance(v, (int, float)) or v <= 0:
                raise ValueError(f"genome_size must be a positive number, got {v}")
        return int(v) if v is not None else v
    
    @field_validator('racon_iter', 'pilon_iter')
    @classmethod
    def validate_iterations(cls, v):
        """Validate iteration counts."""
        if v is not None:
            if not isinstance(v, int) or v < 0:
                raise ValueError(f"Iteration count must be a non-negative integer, got {v}")
        return v
    
    @field_validator('min_contig_len', 'min_contig_cov', 'coverage', 'target_depth')
    @classmethod
    def validate_positive_integers(cls, v):
        """Validate positive integer parameters."""
        if v is not None:
            if not isinstance(v, int) or v <= 0:
                raise ValueError(f"Value must be a positive integer, got {v}")
        return v
    
    @field_validator('genome_size_units')
    @classmethod
    def validate_genome_size_units(cls, v):
        """Validate genome_size_units."""
        if v is not None:
            valid_units = ['bp', 'K', 'M', 'G']
            if v not in valid_units:
                raise ValueError(f"genome_size_units must be one of {valid_units}, got {v}")
        return v
    
    @field_validator('expected_genome_size')
    @classmethod
    def validate_expected_genome_size(cls, v):
        """Validate expected_genome_size."""
        if v is not None:
            if not isinstance(v, (int, float)) or v <= 0:
                raise ValueError(f"expected_genome_size must be a positive number, got {v}")
        return v


class ComprehensiveGenomeAnalysisValidator(BaseStepValidator):
    """
    Validator for ComprehensiveGenomeAnalysis service steps.
    
    Validates step structure and parameters for ComprehensiveGenomeAnalysis.
    Ensures that default parameters are present.
    """
    
    # Expected default parameters (should match ComprehensiveGenomeAnalysisDefaults)
    REQUIRED_DEFAULTS = {
        "genome_size",
        "normalize",
        "trim",
        "coverage",
        "expected_genome_size",
        "genome_size_units",
        "racon_iter",
        "pilon_iter",
        "min_contig_len",
        "min_contig_cov",
        "filtlong",
        "target_depth",
    }
    
    def validate_params(
        self,
        params: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        """
        Validate ComprehensiveGenomeAnalysis step parameters using Pydantic model.
        
        Also validates that all expected default parameters are present.
        
        Args:
            params: Step parameters dictionary
            app_name: Application name
        
        Returns:
            ValidationResult with validated params, warnings, and errors
        """
        warnings = []
        errors = []
        validated_params = params.copy()
        
        # First, check that required defaults are present
        missing_defaults = []
        for default_key in self.REQUIRED_DEFAULTS:
            if default_key not in params:
                missing_defaults.append(default_key)
        
        if missing_defaults:
            errors.append(
                f"Missing required default parameters: {', '.join(missing_defaults)}. "
                "These should be provided by the defaults provider."
            )
        
        # Check that at least one input source is provided
        has_input = (
            params.get('srr_ids') or 
            params.get('paired_end_libs') or 
            params.get('single_end_libs')
        )
        if not has_input:
            errors.append(
                "At least one input source must be provided: "
                "'srr_ids', 'paired_end_libs', or 'single_end_libs'"
            )
        
        # Check for multiple input sources (warn, not error)
        input_count = sum([
            1 if params.get('srr_ids') else 0,
            1 if params.get('paired_end_libs') else 0,
            1 if params.get('single_end_libs') else 0
        ])
        if input_count > 1:
            warnings.append(
                "Multiple input sources provided. "
                "Only one input source should be specified."
            )
        
        try:
            # Validate using Pydantic model
            validated_model = ComprehensiveGenomeAnalysisParams(**params)
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
            
            # Validate recipe value
            recipe = validated_params.get('recipe', 'auto')
            valid_recipes = ['auto', 'standard', 'plasmid', 'viral']
            if recipe and recipe not in valid_recipes:
                warnings.append(
                    f"Recipe '{recipe}' is not a standard recipe. "
                    f"Valid recipes: {', '.join(valid_recipes)}"
                )
            
            # Check that either scientific_name or taxonomy_id is provided (recommended)
            if not validated_params.get('scientific_name') and not validated_params.get('taxonomy_id'):
                warnings.append(
                    "Neither 'scientific_name' nor 'taxonomy_id' is provided. "
                    "At least one is recommended for proper annotation."
                )
            
            # Validate output_path
            output_path = validated_params.get('output_path', '')
            if output_path and not output_path.startswith('${') and not output_path.startswith('/'):
                warnings.append(
                    f"output_path '{output_path}' doesn't appear to be a valid path "
                    "(should start with '/' or be a variable reference)"
                )
            
            logger.debug(
                f"ComprehensiveGenomeAnalysis: Validated {len(validated_params)} parameters"
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
                f"ComprehensiveGenomeAnalysis: Validation failed with {len(errors)} error(s)"
            )
        except Exception as e:
            # Unexpected errors during validation
            errors.append(f"Unexpected validation error: {str(e)}")
            logger.error(
                f"ComprehensiveGenomeAnalysis: Unexpected validation error: {e}",
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
        Validate ComprehensiveGenomeAnalysis step outputs.
        
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
        
        # ComprehensiveGenomeAnalysis-specific output validation
        # Common output keys for ComprehensiveGenomeAnalysis (combines assembly + annotation)
        expected_outputs = [
            'genome_object', 'contigs_fasta', 'annotation_report', 
            'genbank_file', 'gff_file'
        ]
        
        for key in outputs.keys():
            # Check if output key is recognized
            if key not in expected_outputs:
                warnings.append(
                    f"Output key '{key}' is not a standard ComprehensiveGenomeAnalysis output. "
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

