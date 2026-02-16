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

# ComprehensiveGenomeAnalysis enum allowlists from app spec.
CGA_INPUT_TYPES = {"reads", "contigs", "genbank"}
CGA_RECIPES = {
    "auto",
    "unicycler",
    "canu",
    "spades",
    "meta-spades",
    "plasmid-spades",
    "single-cell",
    "flye",
}
CGA_DOMAINS = {"Bacteria", "Archaea", "Viruses", "auto"}
CGA_CODES = {0, 1, 4, 11, 25}

# Normalize common legacy/LLM variants to canonical ComprehensiveGenomeAnalysis values.
CGA_INPUT_TYPE_ALIASES = {
    "read": "reads",
    "reads": "reads",
    "raw_reads": "reads",
    "fastq": "reads",
    "read_file": "reads",
    "contig": "contigs",
    "contigs": "contigs",
    "assembled_contigs": "contigs",
    "contig_file": "contigs",
    "genbank": "genbank",
    "gbk": "genbank",
    "genbank_file": "genbank",
}
CGA_RECIPE_ALIASES = {
    "meta_flye": "flye",
    "meta-flye": "flye",
    "metaflye": "flye",
    "single_cell": "single-cell",
    "meta_spades": "meta-spades",
    "plasmid_spades": "plasmid-spades",
}
CGA_DOMAIN_ALIASES = {
    "bacteria": "Bacteria",
    "bacterial": "Bacteria",
    "archaea": "Archaea",
    "archaeal": "Archaea",
    "virus": "Viruses",
    "viruses": "Viruses",
    "viral": "Viruses",
    "auto": "auto",
}
CGA_CODE_ALIASES = {
    "11 (archaea & most bacteria)": 11,
    "4 (mycoplasma, spiroplasma, & ureaplasma )": 4,
    "25 (candidate division sr1 & gracilibacteria)": 25,
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
    "comprehensivegenomeanalysis": {
        "paired_end_libs": _coerce_to_list,
        "single_end_libs": _coerce_to_list,
        "srr_ids": _coerce_to_list,
        "taxonomy_id": lambda v: int(_coerce_to_number(v)) if v not in (None, "", []) else v,
        "code": lambda v: int(_coerce_to_number(v)) if v not in (None, "", []) else v,
        "racon_iter": lambda v: int(_coerce_to_number(v)) if v not in (None, "", []) else v,
        "pilon_iter": lambda v: int(_coerce_to_number(v)) if v not in (None, "", []) else v,
        "target_depth": lambda v: int(_coerce_to_number(v)) if v not in (None, "", []) else v,
        "min_contig_len": lambda v: int(_coerce_to_number(v)) if v not in (None, "", []) else v,
        "min_contig_cov": _coerce_to_number,
        "genome_size": _coerce_to_number,
        "trim": _coerce_to_bool,
        "normalize": _coerce_to_bool,
        "filtlong": _coerce_to_bool,
        "public": _coerce_to_bool,
        "queue_nowait": _coerce_to_bool,
        "skip_indexing": _coerce_to_bool,
        "analyze_quality": _coerce_to_bool,
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
    "comprehensivegenomeanalysis": {
        "tax_id": "taxonomy_id",
        "taxon_id": "taxonomy_id",
        "taxonid": "taxonomy_id",
        "srr_accession": "srr_ids",
        "srr_accessions": "srr_ids",
        "output_folder": "output_path",
        "output_name": "output_file",
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
    "comprehensivegenomeanalysis": [
        {
            "field": "input_type",
            "equals": "reads",
            "required_one_of": ["paired_end_libs", "single_end_libs", "srr_ids"],
            "message": (
                "When input_type is 'reads', at least one reads source must be provided: "
                "paired_end_libs, single_end_libs, or srr_ids."
            ),
        },
        {
            "field": "input_type",
            "equals": "contigs",
            "required_one_of": ["contigs", "reference_assembly"],
            "message": (
                "When input_type is 'contigs', at least one contig source must be provided: "
                "contigs or reference_assembly."
            ),
        },
        {
            "field": "input_type",
            "equals": "genbank",
            "required_one_of": ["genbank_file", "gto"],
            "message": (
                "When input_type is 'genbank', at least one GenBank source must be provided: "
                "genbank_file or gto."
            ),
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


def _normalize_cga_enum_aliases(step_app: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize known ComprehensiveGenomeAnalysis enum aliases."""
    if not isinstance(params, dict):
        return params
    service_key = _normalize_service_name(step_app)
    if service_key != "comprehensivegenomeanalysis":
        return params

    normalized = dict(params)

    input_type = normalized.get("input_type")
    if isinstance(input_type, str):
        candidate = input_type.strip().lower()
        canonical = CGA_INPUT_TYPE_ALIASES.get(candidate, candidate)
        if canonical != input_type:
            normalized["input_type"] = canonical
            logger.info(
                "Normalized ComprehensiveGenomeAnalysis input_type: %r -> %r",
                input_type,
                canonical,
            )

    recipe = normalized.get("recipe")
    if isinstance(recipe, str):
        candidate = recipe.strip().lower()
        canonical = CGA_RECIPE_ALIASES.get(candidate, candidate)
        if canonical != recipe:
            normalized["recipe"] = canonical
            logger.info(
                "Normalized ComprehensiveGenomeAnalysis recipe: %r -> %r",
                recipe,
                canonical,
            )

    domain = normalized.get("domain")
    if isinstance(domain, str):
        candidate = domain.strip().lower()
        canonical = CGA_DOMAIN_ALIASES.get(candidate, domain.strip())
        if canonical != domain:
            normalized["domain"] = canonical
            logger.info(
                "Normalized ComprehensiveGenomeAnalysis domain: %r -> %r",
                domain,
                canonical,
            )

    code = normalized.get("code")
    if isinstance(code, str):
        candidate = code.strip()
        if candidate in CGA_CODE_ALIASES:
            canonical = CGA_CODE_ALIASES[candidate]
            normalized["code"] = canonical
            logger.info(
                "Normalized ComprehensiveGenomeAnalysis code: %r -> %r",
                code,
                canonical,
            )

    return normalized


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
    params = _normalize_cga_enum_aliases(step_app, params)
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
        required_one_of = rule.get("required_one_of", [])
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

        if required_one_of:
            present = any(
                field in params
                and params.get(field) not in (None, "", [])
                and not (
                    isinstance(params.get(field), str)
                    and params.get(field).strip() == ""
                )
                for field in required_one_of
            )
            if not present:
                message = rule.get("message") or (
                    f"When {condition_field} is {condition_value!r}, "
                    f"at least one of {', '.join(required_one_of)} must be provided."
                )
                errors.append(message)

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

    # ComprehensiveGenomeAnalysis: strict enum allowlists and input compatibility rules.
    if service_key == "comprehensivegenomeanalysis":
        input_type = params.get("input_type")
        if isinstance(input_type, str):
            candidate = input_type.strip().lower()
        else:
            candidate = input_type
        if candidate not in CGA_INPUT_TYPES:
            errors.append(
                "input_type must be one of: reads, contigs, genbank. "
                f"Got: {input_type!r}."
            )

        recipe = params.get("recipe")
        if recipe is not None:
            recipe_candidate = recipe.strip().lower() if isinstance(recipe, str) else recipe
            if recipe_candidate not in CGA_RECIPES:
                errors.append(
                    "recipe must be one of: "
                    f"{', '.join(sorted(CGA_RECIPES))}. Got: {recipe!r}."
                )

        domain = params.get("domain")
        if domain is not None and domain not in CGA_DOMAINS:
            errors.append(
                "domain must be one of: "
                f"{', '.join(sorted(CGA_DOMAINS))}. Got: {domain!r}."
            )

        code = params.get("code")
        if code is not None:
            code_candidate = int(code) if isinstance(code, (int, float, str)) and str(code).strip().isdigit() else code
            if code_candidate not in CGA_CODES:
                errors.append(
                    f"code must be one of: {sorted(CGA_CODES)}. Got: {code!r}."
                )

        # Catch ambiguous payloads where fields conflict with the declared input_type.
        if candidate == "reads":
            conflicts = [
                name for name in ("contigs", "genbank_file", "gto")
                if params.get(name) not in (None, "", [])
            ]
            if conflicts:
                errors.append(
                    "When input_type is 'reads', do not provide contigs/genbank inputs. "
                    f"Conflicting fields: {', '.join(conflicts)}."
                )
        elif candidate == "contigs":
            conflicts = [
                name for name in ("paired_end_libs", "single_end_libs", "srr_ids", "genbank_file", "gto")
                if params.get(name) not in (None, "", [])
            ]
            if conflicts:
                errors.append(
                    "When input_type is 'contigs', do not provide reads or genbank inputs. "
                    f"Conflicting fields: {', '.join(conflicts)}."
                )
        elif candidate == "genbank":
            conflicts = [
                name for name in ("paired_end_libs", "single_end_libs", "srr_ids", "contigs", "reference_assembly")
                if params.get(name) not in (None, "", [])
            ]
            if conflicts:
                errors.append(
                    "When input_type is 'genbank', do not provide reads/contigs assembly inputs. "
                    f"Conflicting fields: {', '.join(conflicts)}."
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

