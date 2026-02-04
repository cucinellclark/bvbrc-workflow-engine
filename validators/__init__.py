"""
Service-specific step validators and defaults for adding defaults and validating parameters.

This module provides:
- Base classes for defaults providers and validators
- Registry systems for managing service-specific implementations
- Separate concerns: defaults (additive) and validation (strict)
"""
from .base_defaults import BaseDefaults
from .defaults_registry import (
    DefaultsRegistry,
    register_defaults,
    get_defaults,
    get_defaults_registry
)
from .base_validator import BaseStepValidator, ValidationResult
from .validator_registry import (
    ValidatorRegistry,
    register_validator,
    get_validator,
    get_registry
)

# Auto-register built-in defaults and validators
try:
    from .genome_annotation_defaults import GenomeAnnotationDefaults
    register_defaults("GenomeAnnotation", GenomeAnnotationDefaults)
except ImportError:
    # Defaults not available, skip registration
    pass

try:
    from .genome_annotation_validator import GenomeAnnotationValidator
    register_validator("GenomeAnnotation", GenomeAnnotationValidator)
except ImportError:
    # Validator not available, skip registration
    pass

try:
    from .comprehensive_genome_analysis_defaults import ComprehensiveGenomeAnalysisDefaults
    register_defaults("ComprehensiveGenomeAnalysis", ComprehensiveGenomeAnalysisDefaults)
except ImportError:
    # Defaults not available, skip registration
    pass

try:
    from .comprehensive_genome_analysis_validator import ComprehensiveGenomeAnalysisValidator
    register_validator("ComprehensiveGenomeAnalysis", ComprehensiveGenomeAnalysisValidator)
except ImportError:
    # Validator not available, skip registration
    pass

__all__ = [
    # Defaults
    "BaseDefaults",
    "DefaultsRegistry",
    "register_defaults",
    "get_defaults",
    "get_defaults_registry",
    # Validators
    "BaseStepValidator",
    "ValidationResult",
    "ValidatorRegistry",
    "register_validator",
    "get_validator",
    "get_registry",
]

