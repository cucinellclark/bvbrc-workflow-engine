"""Utility functions for cleaning and normalizing workflow JSON before validation."""
from typing import Dict, Any, List
from utils.logger import get_logger


logger = get_logger(__name__)



def _is_comprehensive_genome_analysis(app: str) -> bool:
    """Check if app name is ComprehensiveGenomeAnalysis (case-insensitive)."""
    if not app:
        return False
    app_lower = app.strip().lower().replace('-', '_')
    return app_lower in ('comprehensivegenomeanalysis', 'comprehensive_genome_analysis')


def _is_taxonomic_classification(app: str) -> bool:
    """Check if app name is TaxonomicClassification (case-insensitive)."""
    if not app:
        return False
    app_lower = app.strip().lower().replace('-', '_')
    return app_lower in ('taxonomicclassification', 'taxonomic_classification')


def clean_empty_optional_lists(workflow_data: Dict[str, Any]) -> Dict[str, Any]:
    """Remove empty lists from optional fields that should not be empty if present.
    
    Some validators check if optional list fields are empty and raise errors.
    If these fields are present but empty, we remove them so validation can proceed.
    The validators will then check if required fields are present based on input_type
    or other conditions.
    
    Args:
        workflow_data: Workflow dictionary that may contain empty lists
        
    Returns:
        Workflow dictionary with empty lists removed from optional fields
    """
    cleaned_workflow = _deep_copy(workflow_data)
    
    steps = cleaned_workflow.get('steps', [])
    for step in steps:
        app = step.get('app', '')
        params = step.get('params', {})
        
        # Fields that should be removed if empty (app-specific)
        fields_to_clean = []
        
        if _is_comprehensive_genome_analysis(app):
            # These fields are optional but cannot be empty lists if present
            fields_to_clean = ['paired_end_libs', 'single_end_libs', 'srr_ids']
        elif _is_taxonomic_classification(app):
            # These fields are optional but cannot be empty lists if present
            fields_to_clean = ['paired_end_libs', 'single_end_libs', 'srr_libs']
        
        # Remove empty lists from optional fields
        for field in fields_to_clean:
            if field in params:
                value = params[field]
                if isinstance(value, list) and len(value) == 0:
                    logger.info(
                        f"Removing empty list for optional field '{field}' in step '{step.get('step_name', 'unknown')}' "
                        f"(app: {app})"
                    )
                    params.pop(field)
    
    return cleaned_workflow


def _deep_copy(obj: Any) -> Any:
    """Create a deep copy of an object."""
    if isinstance(obj, dict):
        return {key: _deep_copy(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_deep_copy(item) for item in obj]
    else:
        return obj

