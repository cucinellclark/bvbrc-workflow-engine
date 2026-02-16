"""Workflow JSON validation and dependency checking."""
from typing import Dict, List, Set, Any, Optional
import re

from pydantic import ValidationError
from models.workflow import WorkflowDefinition, WorkflowStep
from utils.logger import get_logger
from validators import get_defaults, get_validator
from utils.output_file_checker import check_and_resolve_output_conflicts
from core.field_coercion_registry import (
    coerce_workflow_definition,
    validate_workflow_service_field_rules,
)


logger = get_logger(__name__)

# Canonical BV-BRC AppService IDs by friendly/service alias.
# This allows workflow ingestion to accept user-friendly lowercase names
# while ensuring submitted steps use consistent app IDs.
FRIENDLY_TO_APP_ID = {
    "date": "Date",
    "genome_assembly": "GenomeAssembly2",
    "genome_annotation": "GenomeAnnotation",
    "comprehensive_genome_analysis": "ComprehensiveGenomeAnalysis",
    "blast": "Homology",
    "primer_design": "PrimerDesign",
    "variation": "Variation",
    "tnseq": "TnSeq",
    "bacterial_genome_tree": "CodonTree",
    "gene_tree": "GeneTree",
    "core_genome_mlst": "CoreGenomeMLST",
    "whole_genome_snp": "WholeGenomeSNPAnalysis",
    "taxonomic_classification": "TaxonomicClassification",
    "metagenomic_binning": "MetagenomeBinning",
    "metagenomic_read_mapping": "MetagenomicReadMapping",
    "rnaseq": "RNASeq",
    "expression_import": "ExpressionImport",
    "sars_wastewater_analysis": "SARSWastewaterAnalysis",
    "sequence_submission": "SequenceSubmission",
    "influenza_ha_subtype_conversion": "InfluenzaHASubtypeConversion",
    "subspecies_classification": "SubspeciesClassification",
    "viral_assembly": "ViralAssembly",
    "genome_alignment": "GenomeAlignment",
    "sars_genome_analysis": "SARS2Assembly",
    "msa_snp_analysis": "MSA",
    "metacats": "MetaCATS",
    "proteome_comparison": "GenomeComparison",
    "comparative_systems": "ComparativeSystems",
    "docking": "Docking",
    "similar_genome_finder": "SimilarGenomeFinder",
    "fastqutils": "FastqUtils",
}

# Extra aliases observed in service names/tools.
EXTRA_APP_ALIASES = {
    "hasubtypenumberingconversion": "InfluenzaHASubtypeConversion",
}


class WorkflowValidator:
    """Validates workflow JSON and business logic."""

    @staticmethod
    def _normalize_step_app_name(app_name: str) -> str:
        """Normalize step.app values into BV-BRC AppService app IDs when possible.

        In practice we sometimes receive service-style snake_case names
        (e.g. "taxonomic_classification") instead of AppService IDs
        (e.g. "TaxonomicClassification"). If we can confidently map/convert
        to a registered validator/defaults target, do so.
        """
        if not app_name or not isinstance(app_name, str):
            return app_name
        app_name = app_name.strip()

        # If it already matches a registered validator/defaults, keep it.
        if get_validator(app_name) or get_defaults(app_name):
            return app_name

        lower_name = app_name.lower()

        # 1) Friendly name aliases (snake_case service names).
        if lower_name in FRIENDLY_TO_APP_ID:
            return FRIENDLY_TO_APP_ID[lower_name]

        # 2) Case-insensitive exact match against known App IDs.
        app_ids = set(FRIENDLY_TO_APP_ID.values())
        app_ids.update(EXTRA_APP_ALIASES.values())
        for app_id in app_ids:
            if lower_name == app_id.lower():
                return app_id

        # 3) Extra explicit aliases.
        if lower_name in EXTRA_APP_ALIASES:
            return EXTRA_APP_ALIASES[lower_name]

        # Conservative snake_case -> TitleCase conversion, but ONLY if it maps
        # to a registered validator/defaults target.
        if "_" in app_name:
            candidate = "".join(part[:1].upper() + part[1:] for part in app_name.split("_") if part)
            if candidate and (get_validator(candidate) or get_defaults(candidate)):
                return candidate

        return app_name
    
    @staticmethod
    def validate_workflow_input(
        workflow_data: Dict[str, Any],
        auth_token: Optional[str] = None
    ) -> WorkflowDefinition:
        """Validate workflow input JSON.
        
        Args:
            workflow_data: Raw workflow dictionary
            auth_token: Optional authentication token for workspace API calls
            
        Returns:
            Validated WorkflowDefinition object
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Check that workflow_id is NOT present (input format)
            if 'workflow_id' in workflow_data:
                raise ValueError(
                    "Input workflow should not contain 'workflow_id'. "
                    "IDs are assigned by the scheduler."
                )
            
            # Check that step_id is NOT present in any step
            for step in workflow_data.get('steps', []):
                if 'step_id' in step:
                    raise ValueError(
                        f"Input step '{step.get('step_name')}' should not "
                        f"contain 'step_id'. IDs are assigned by the scheduler."
                    )
            
            # Apply field coercion BEFORE schema validation to fix common type errors
            logger.info("Applying field type coercion rules")
            workflow_data = coerce_workflow_definition(workflow_data)

            # Apply cross-field service rules after coercion and before schema validation.
            # This gives actionable errors for conditional requirements.
            service_field_errors = validate_workflow_service_field_rules(workflow_data)
            if service_field_errors:
                raise ValueError(
                    "Service field validation failed:\n  - "
                    + "\n  - ".join(service_field_errors)
                )
            
            # Validate using Pydantic model
            logger.info("Validating workflow schema")
            workflow = WorkflowDefinition(**workflow_data)
            
            # Apply service-specific defaults and validate steps
            # This modifies workflow_data and returns updated workflow
            workflow = WorkflowValidator._apply_step_validators(workflow_data, workflow)
            
            # Check and resolve output file conflicts
            if auth_token:
                logger.info("Checking for output file conflicts")
                workflow_data = check_and_resolve_output_conflicts(
                    workflow_data,
                    auth_token
                )
                # Re-validate with updated workflow_data
                workflow = WorkflowDefinition(**workflow_data)
            else:
                logger.info(
                    "No auth token provided - skipping output file conflict check"
                )
            
            # Additional business logic validation
            WorkflowValidator.validate_step_dependencies(workflow.steps)
            WorkflowValidator.validate_variable_references(workflow)
            
            logger.info(
                f"Workflow '{workflow.workflow_name}' validation successful"
            )
            return workflow
            
        except ValidationError as e:
            logger.error(f"Schema validation failed: {e}")
            raise ValueError(f"Schema validation failed: {str(e)}")
        except ValueError as e:
            logger.error(f"Validation failed: {e}")
            raise
    
    @staticmethod
    def validate_step_dependencies(steps: List[WorkflowStep]) -> None:
        """Validate step dependencies.
        
        Checks:
        - All dependencies reference valid step names
        - No circular dependencies exist
        
        Args:
            steps: List of workflow steps
            
        Raises:
            ValueError: If dependencies are invalid
        """
        logger.debug("Validating step dependencies")
        
        # Build step name set
        step_names = {step.step_name for step in steps}
        
        # Check all depends_on references are valid
        for step in steps:
            if step.depends_on:
                for dep in step.depends_on:
                    if dep not in step_names:
                        raise ValueError(
                            f"Step '{step.step_name}' depends on "
                            f"unknown step '{dep}'"
                        )
        
        # Check for circular dependencies using DFS
        WorkflowValidator._check_circular_dependencies(steps)
        
        logger.debug("Step dependencies validated")
    
    @staticmethod
    def _check_circular_dependencies(steps: List[WorkflowStep]) -> None:
        """Check for circular dependencies in workflow steps.
        
        Uses depth-first search to detect cycles.
        
        Args:
            steps: List of workflow steps
            
        Raises:
            ValueError: If circular dependency detected
        """
        # Build dependency graph
        graph: Dict[str, List[str]] = {}
        for step in steps:
            graph[step.step_name] = step.depends_on or []
        
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        
        def dfs(node: str, path: List[str]) -> None:
            """Depth-first search to detect cycles."""
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    dfs(neighbor, path.copy())
                elif neighbor in rec_stack:
                    # Circular dependency found
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    raise ValueError(
                        f"Circular dependency detected: "
                        f"{' -> '.join(cycle)}"
                    )
            
            rec_stack.remove(node)
        
        # Run DFS from each node
        for step_name in graph.keys():
            if step_name not in visited:
                dfs(step_name, [])
    
    @staticmethod
    def validate_variable_references(workflow: WorkflowDefinition) -> None:
        """Validate variable reference syntax.
        
        Checks template strings like:
        - ${workspace_root}
        - ${steps.step_name.outputs.output_name}
        - ${params.param_name}
        
        Args:
            workflow: Workflow definition
            
        Raises:
            ValueError: If variable references are invalid
        """
        logger.debug("Validating variable references")
        
        step_names = {step.step_name for step in workflow.steps}
        
        # Pattern to match variable references
        var_pattern = re.compile(r'\$\{([^}]+)\}')
        
        def check_string(value: str, context: str) -> None:
            """Check a string for valid variable references."""
            matches = var_pattern.findall(value)
            for match in matches:
                parts = match.split('.')
                
                # Check step references
                if len(parts) >= 2 and parts[0] == 'steps':
                    step_ref = parts[1]
                    if step_ref not in step_names:
                        raise ValueError(
                            f"In {context}: Variable reference "
                            f"'${{{match}}}' refers to unknown step "
                            f"'{step_ref}'"
                        )
        
        def check_value(value: Any, context: str) -> None:
            """Recursively check values for variable references."""
            if isinstance(value, str):
                check_string(value, context)
            elif isinstance(value, dict):
                for k, v in value.items():
                    check_value(v, f"{context}.{k}")
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    check_value(item, f"{context}[{i}]")
        
        # Check all steps
        for step in workflow.steps:
            context = f"step '{step.step_name}'"
            
            # Check params
            check_value(step.params, f"{context}.params")
            
            # Check outputs
            if step.outputs:
                check_value(step.outputs, f"{context}.outputs")
        
        # Check workflow outputs
        if workflow.workflow_outputs:
            for i, output in enumerate(workflow.workflow_outputs):
                check_string(output, f"workflow_outputs[{i}]")
        
        logger.debug("Variable references validated")
    
    @staticmethod
    def _apply_step_validators(
        workflow_data: Dict[str, Any],
        workflow: WorkflowDefinition
    ) -> WorkflowDefinition:
        """Apply service-specific defaults and validators to workflow steps.
        
        This method:
        1. Applies default parameters (non-destructive) using defaults providers
        2. Validates step parameters and structure using validators
        3. Updates step params with enriched/validated values
        4. Returns a new WorkflowDefinition with updated steps
        
        Args:
            workflow_data: Raw workflow dictionary (will be modified)
            workflow: Current WorkflowDefinition object
        
        Returns:
            New WorkflowDefinition with enriched and validated steps
        
        Raises:
            ValueError: If validation fails for any step
        """
        logger.debug("Applying service-specific defaults and validators")
        
        validation_errors = []
        validation_warnings = []
        
        for step_dict in workflow_data['steps']:
            step_name = step_dict.get('step_name', 'unknown')
            original_app_name = step_dict.get('app', '')
            app_name = WorkflowValidator._normalize_step_app_name(original_app_name)
            if app_name != original_app_name:
                logger.info(
                    f"Step '{step_name}': Normalized app name '{original_app_name}' -> '{app_name}'"
                )
                step_dict['app'] = app_name
            
            if not app_name:
                logger.warning(f"Step '{step_name}' has no app name, skipping")
                continue
            
            try:
                # Step 1: Apply defaults (if defaults provider exists)
                defaults_provider = get_defaults(app_name)
                if defaults_provider:
                    logger.debug(
                        f"Step '{step_name}': Applying defaults for app '{app_name}'"
                    )
                    step_dict['params'] = defaults_provider.apply_defaults(
                        step_dict.get('params', {}),
                        app_name
                    )
                else:
                    logger.debug(
                        f"Step '{step_name}': No defaults provider for app '{app_name}'"
                    )
                
                # Step 2: Validate (if validator exists)
                validator = get_validator(app_name)
                if validator:
                    logger.debug(
                        f"Step '{step_name}': Validating for app '{app_name}'"
                    )
                    result = validator.validate_step(step_dict, app_name)
                    
                    # Check for errors
                    if result.has_errors():
                        for error in result.errors:
                            validation_errors.append(
                                f"Step '{step_name}' ({app_name}): {error}"
                            )
                    
                    # Collect warnings
                    if result.has_warnings():
                        for warning in result.warnings:
                            validation_warnings.append(
                                f"Step '{step_name}' ({app_name}): {warning}"
                            )
                    
                    # Update step params with validated params
                    step_dict['params'] = result.params
                    
                    logger.debug(
                        f"Step '{step_name}': Validation complete "
                        f"({len(result.warnings)} warnings, {len(result.errors)} errors)"
                    )
                else:
                    logger.debug(
                        f"Step '{step_name}': No validator for app '{app_name}', "
                        "skipping validation"
                    )
                
            except Exception as e:
                # Unexpected error during defaults/validation
                error_msg = f"Step '{step_name}' ({app_name}): Unexpected error: {str(e)}"
                validation_errors.append(error_msg)
                logger.error(error_msg, exc_info=True)
        
        # Log warnings (non-critical)
        if validation_warnings:
            for warning in validation_warnings:
                logger.warning(warning)
        
        # Raise exception if there are validation errors
        if validation_errors:
            error_summary = "\n".join(f"  - {err}" for err in validation_errors)
            raise ValueError(
                f"Step validation failed with {len(validation_errors)} error(s):\n{error_summary}"
            )
        
        # Reconstruct workflow with updated steps
        # Create new WorkflowDefinition from modified workflow_data
        validated_workflow = WorkflowDefinition(**workflow_data)
        
        logger.debug("Service-specific defaults and validators applied successfully")
        return validated_workflow

