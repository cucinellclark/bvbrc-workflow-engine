"""Field coercion registry for workflow parameter type corrections.

This module provides automatic type coercion for workflow step parameters
to prevent common LLM generation errors (like strings instead of lists).

Pattern-based and service-specific rules ensure that workflow submissions
don't fail due to trivial type mismatches.
"""
from typing import Dict, List, Any, Callable, Pattern, Tuple
import re
from utils.logger import get_logger

logger = get_logger(__name__)

# Known valid Homology precomputed database IDs.
HOMOLOGY_PRECOMPUTED_DATABASES = {
    "bacteria-archaea",
    "viral-reference",
}

# Normalize common legacy/LLM variants to canonical Homology DB IDs.
HOMOLOGY_PRECOMPUTED_DB_ALIASES = {
    "patric": "bacteria-archaea",
    "bacteria_archaea": "bacteria-archaea",
    "bacteria archaea": "bacteria-archaea",
    "viral_reference": "viral-reference",
    "viral reference": "viral-reference",
}


# ============================================================================
# Pattern-Based Coercion Rules (apply to all services)
# ============================================================================

def _coerce_to_list(value: Any) -> List[Any]:
    """Convert value to list if not already."""
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _coerce_to_number(value: Any) -> float:
    """Convert value to number if possible."""
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            # Try int first
            if '.' not in value and 'e' not in value.lower():
                return int(value)
            return float(value)
        except (ValueError, TypeError):
            pass
    return value


def _coerce_to_bool(value: Any) -> bool:
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', 'yes', '1', 'on')
    return bool(value)


# Pattern-based rules: regex -> coercion function
PATTERN_RULES: List[Tuple[Pattern[str], Callable[[Any], Any]]] = [
    # Any field ending in _id_list, _ids, _list should be a list
    (re.compile(r'^.*_(id_list|ids|list)$', re.IGNORECASE), _coerce_to_list),
    
    # Genome/taxon/feature collections
    (re.compile(r'^(genome_ids|taxon_ids|feature_ids|genomes|taxa|features)$', re.IGNORECASE), _coerce_to_list),
    
    # Library fields (paired_end_libs, single_end_libs, etc.)
    (re.compile(r'^.*_libs$', re.IGNORECASE), _coerce_to_list),
    
    # Groups field
    (re.compile(r'^groups$', re.IGNORECASE), _coerce_to_list),
    
    # Contrasts and experimental conditions
    (re.compile(r'^(contrasts|experimental_conditions)$', re.IGNORECASE), _coerce_to_list),
    
]


# ============================================================================
# Service-Specific Coercion Rules
# ============================================================================

# Format: {normalized_service_name: {field_name: coercion_function}}
# Normalization removes non-alphanumerics and lowercases, so
# "TaxonomicClassification", "taxonomic_classification", and
# "taxonomic-classification" resolve to the same key.
SERVICE_FIELD_RULES: Dict[str, Dict[str, Callable[[Any], Any]]] = {
    "homology": {  # blast service
        "input_id_list": _coerce_to_list,
        "db_id_list": _coerce_to_list,
        "db_genome_list": _coerce_to_list,
        "db_taxon_list": _coerce_to_list,
        "blast_evalue_cutoff": _coerce_to_number,
        "blast_max_hits": lambda v: int(_coerce_to_number(v)) if v else v,
    },
    "blast": {  # alias
        "input_id_list": _coerce_to_list,
        "db_id_list": _coerce_to_list,
        "db_genome_list": _coerce_to_list,
        "db_taxon_list": _coerce_to_list,
        "blast_evalue_cutoff": _coerce_to_number,
        "blast_max_hits": lambda v: int(_coerce_to_number(v)) if v else v,
    },
    "taxonomicclassification": {
        "paired_end_libs": _coerce_to_list,
        "single_end_libs": _coerce_to_list,
        "srr_libs": _coerce_to_list,
        "confidence_interval": _coerce_to_number,
    },
    "genomeassembly2": {
        "paired_end_libs": _coerce_to_list,
        "single_end_libs": _coerce_to_list,
        "srr_ids": _coerce_to_list,
        "racon_iter": lambda v: int(_coerce_to_number(v)) if v else v,
        "pilon_iter": lambda v: int(_coerce_to_number(v)) if v else v,
        "min_contig_len": lambda v: int(_coerce_to_number(v)) if v else v,
        "min_contig_cov": lambda v: int(_coerce_to_number(v)) if v else v,
    },
    "codontree": {
        "genome_ids": _coerce_to_list,
        "genome_groups": _coerce_to_list,
        "optional_genome_ids": _coerce_to_list,
    },
    "rnaseq": {
        "paired_end_libs": _coerce_to_list,
        "single_end_libs": _coerce_to_list,
        "srr_libs": _coerce_to_list,
        "strand_specific": _coerce_to_bool,
        "trimming": _coerce_to_bool,
    },
    "variation": {
        "paired_end_libs": _coerce_to_list,
        "single_end_libs": _coerce_to_list,
        "srr_ids": _coerce_to_list,
        "debug": _coerce_to_bool,
    },
    "metagenomebinning": {
        "paired_end_libs": _coerce_to_list,
        "single_end_libs": _coerce_to_list,
        "srr_ids": _coerce_to_list,
        "min_contig_len": lambda v: int(_coerce_to_number(v)) if v else v,
        "min_contig_cov": lambda v: int(_coerce_to_number(v)) if v else v,
    },
    "genomealignment": {
        "genome_ids": _coerce_to_list,
    },
}

# Field aliases by service to normalize common LLM variants.
# Format: {normalized_service_name: {alias_field: canonical_field}}
SERVICE_FIELD_ALIASES: Dict[str, Dict[str, str]] = {
    "homology": {
        "precomputed_database": "db_precomputed_database",
        "db_precomputed_db": "db_precomputed_database",
    },
    "blast": {
        "precomputed_database": "db_precomputed_database",
        "db_precomputed_db": "db_precomputed_database",
    },
}

# Conditional requirements by service.
# Format:
# {
#   normalized_service_name: [
#     {"field": "x", "equals": "y", "required": ["a", "b"], "message": "..."}
#   ]
# }
CONDITIONAL_REQUIRED_RULES: Dict[str, List[Dict[str, Any]]] = {
    "homology": [
        {
            "field": "db_source",
            "equals": "precomputed_database",
            "required": ["db_precomputed_database"],
            "message": (
                "When db_source is 'precomputed_database', "
                "db_precomputed_database must be provided."
            ),
        },
        {
            "field": "input_source",
            "equals": "id_list",
            "required": ["input_id_list"],
            "message": "When input_source is 'id_list', input_id_list must be provided.",
        },
        {
            "field": "db_source",
            "equals": "id_list",
            "required": ["db_id_list"],
            "message": "When db_source is 'id_list', db_id_list must be provided.",
        },
    ],
    "blast": [
        {
            "field": "db_source",
            "equals": "precomputed_database",
            "required": ["db_precomputed_database"],
            "message": (
                "When db_source is 'precomputed_database', "
                "db_precomputed_database must be provided."
            ),
        },
        {
            "field": "input_source",
            "equals": "id_list",
            "required": ["input_id_list"],
            "message": "When input_source is 'id_list', input_id_list must be provided.",
        },
        {
            "field": "db_source",
            "equals": "id_list",
            "required": ["db_id_list"],
            "message": "When db_source is 'id_list', db_id_list must be provided.",
        },
    ],
}


def _normalize_service_name(service_name: str) -> str:
    """Normalize service names to stable rule keys."""
    if not isinstance(service_name, str):
        return ""
    return re.sub(r"[^a-z0-9]+", "", service_name.lower())


def _apply_field_aliases(step_app: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize known alias fields into canonical parameter names."""
    if not isinstance(params, dict):
        return params
    service_key = _normalize_service_name(step_app)
    alias_map = SERVICE_FIELD_ALIASES.get(service_key, {})
    if not alias_map:
        return params

    normalized = dict(params)
    for alias_field, canonical_field in alias_map.items():
        if alias_field in normalized and canonical_field not in normalized:
            normalized[canonical_field] = normalized.pop(alias_field)
            logger.info(
                "Normalized aliased field for service '%s': '%s' -> '%s'",
                step_app,
                alias_field,
                canonical_field,
            )

    return normalized


def _normalize_homology_precomputed_database(
    step_app: str,
    params: Dict[str, Any]
) -> Dict[str, Any]:
    """Normalize known Homology precomputed database aliases."""
    if not isinstance(params, dict):
        return params
    service_key = _normalize_service_name(step_app)
    if service_key not in {"homology", "blast"}:
        return params

    value = params.get("db_precomputed_database")
    if not isinstance(value, str):
        return params

    canonical_candidate = value.strip().lower()
    canonical = HOMOLOGY_PRECOMPUTED_DB_ALIASES.get(canonical_candidate, canonical_candidate)
    if canonical != value:
        normalized = dict(params)
        normalized["db_precomputed_database"] = canonical
        logger.info(
            "Normalized Homology precomputed DB for service '%s': %r -> %r",
            step_app,
            value,
            canonical,
        )
        return normalized
    return params


# ============================================================================
# Main Coercion Engine
# ============================================================================

def coerce_workflow_step_params(
    step_app: str,
    params: Dict[str, Any],
    strict: bool = False
) -> Dict[str, Any]:
    """Apply field coercion rules to workflow step parameters.
    
    Args:
        step_app: The app/service name (e.g. "Homology", "blast")
        params: The parameters dictionary to coerce
        strict: If True, raise exceptions on coercion failures; if False, log warnings
    
    Returns:
        New params dict with coerced values (does not mutate original)
    """
    if not params or not isinstance(params, dict):
        return params
    
    params = _apply_field_aliases(step_app, params)
    params = _normalize_homology_precomputed_database(step_app, params)
    coerced = {}
    changed_fields = []
    
    for field_name, value in params.items():
        original_value = value
        
        # 1) Try service-specific rules first
        service_rules = SERVICE_FIELD_RULES.get(_normalize_service_name(step_app), {})
        if field_name in service_rules:
            try:
                value = service_rules[field_name](value)
                if value != original_value:
                    changed_fields.append(
                        f"{field_name}: {type(original_value).__name__}({original_value!r}) -> "
                        f"{type(value).__name__}({value!r})"
                    )
            except Exception as e:
                msg = f"Service-specific coercion failed for {step_app}.{field_name}: {e}"
                if strict:
                    raise ValueError(msg) from e
                logger.warning(msg)
        
        # 2) Apply pattern-based rules
        for pattern, coercion_fn in PATTERN_RULES:
            if pattern.match(field_name):
                try:
                    new_value = coercion_fn(value)
                    if new_value != value:
                        if field_name not in service_rules:
                            # Only apply if not already handled by service-specific rule
                            if new_value != original_value:
                                changed_fields.append(
                                    f"{field_name}: {type(value).__name__}({value!r}) -> "
                                    f"{type(new_value).__name__}({new_value!r})"
                                )
                            value = new_value
                except Exception as e:
                    msg = f"Pattern-based coercion failed for {step_app}.{field_name}: {e}"
                    if strict:
                        raise ValueError(msg) from e
                    logger.debug(msg)
                break  # Only apply first matching pattern
        
        coerced[field_name] = value
    
    if changed_fields:
        logger.info(
            f"Coerced {len(changed_fields)} field(s) for service '{step_app}': "
            + ", ".join(changed_fields)
        )
    
    return coerced


def validate_step_service_field_rules(step_app: str, params: Dict[str, Any]) -> List[str]:
    """Validate conditional service+field rules and return step-local errors."""
    if not isinstance(params, dict):
        return []

    errors: List[str] = []
    service_key = _normalize_service_name(step_app)
    rules = CONDITIONAL_REQUIRED_RULES.get(service_key, [])
    if not rules:
        return errors

    for rule in rules:
        condition_field = rule.get("field")
        condition_value = rule.get("equals")
        required_fields = rule.get("required", [])
        if params.get(condition_field) != condition_value:
            continue

        missing = [
            field
            for field in required_fields
            if field not in params 
            or params.get(field) in (None, "", [])
            or (isinstance(params.get(field), str) and params.get(field).strip() == "")
        ]
        if missing:
            message = rule.get("message") or (
                f"When {condition_field} is {condition_value!r}, "
                f"{', '.join(required_fields)} must be provided."
            )
            errors.append(f"{message} Missing: {', '.join(missing)}.")

    # Homology/BLAST: enforce explicit allowlist when using precomputed databases.
    if service_key in {"homology", "blast"} and params.get("db_source") == "precomputed_database":
        precomputed = params.get("db_precomputed_database")
        if isinstance(precomputed, str):
            candidate = precomputed.strip().lower()
        else:
            candidate = precomputed
        if candidate not in HOMOLOGY_PRECOMPUTED_DATABASES:
            allowed = ", ".join(sorted(HOMOLOGY_PRECOMPUTED_DATABASES))
            errors.append(
                "When db_source is 'precomputed_database', db_precomputed_database "
                f"must be one of: {allowed}. Got: {precomputed!r}."
            )

    return errors


def validate_workflow_service_field_rules(workflow: Dict[str, Any]) -> List[str]:
    """Validate conditional service+field rules across all workflow steps."""
    if not isinstance(workflow, dict):
        return []

    errors: List[str] = []
    steps = workflow.get("steps", [])
    if not isinstance(steps, list):
        return errors

    for idx, step in enumerate(steps):
        if not isinstance(step, dict):
            continue
        step_name = step.get("step_name") or f"step[{idx}]"
        step_app = step.get("app", "")
        params = step.get("params", {})
        step_errors = validate_step_service_field_rules(step_app, params)
        for err in step_errors:
            errors.append(f"Step '{step_name}' ({step_app}): {err}")

    return errors


def coerce_workflow_definition(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """Apply field coercion to all steps in a workflow definition.
    
    Args:
        workflow: Workflow definition dict
    
    Returns:
        New workflow dict with coerced step params (does not mutate original)
    """
    if not workflow or not isinstance(workflow, dict):
        return workflow
    
    coerced_workflow = workflow.copy()
    
    if "steps" in workflow and isinstance(workflow["steps"], list):
        coerced_steps = []
        for step in workflow["steps"]:
            if not isinstance(step, dict):
                coerced_steps.append(step)
                continue
            
            coerced_step = step.copy()
            step_app = step.get("app", "")
            
            if "params" in step and isinstance(step["params"], dict):
                coerced_step["params"] = coerce_workflow_step_params(
                    step_app=step_app,
                    params=step["params"],
                    strict=False
                )
            
            coerced_steps.append(coerced_step)
        
        coerced_workflow["steps"] = coerced_steps
    
    return coerced_workflow

