"""ComprehensiveGenomeAnalysis service step validator."""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator, ValidationError
import logging

from .base_validator import BaseStepValidator, ValidationResult

logger = logging.getLogger(__name__)

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
CGA_PLATFORMS = {"infer", "illumina", "pacbio", "pacbio_hifi", "nanopore"}
CGA_DOMAINS = {"Bacteria", "Archaea", "Viruses", "auto"}
CGA_CODES = {0, 1, 4, 11, 25}

CGA_INPUT_TYPE_ALIASES = {
    "read": "reads",
    "reads": "reads",
    "raw_reads": "reads",
    "fastq": "reads",
    "contig": "contigs",
    "contigs": "contigs",
    "assembled_contigs": "contigs",
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
CGA_PLATFORM_ALIASES = {
    "pacbio-hifi": "pacbio_hifi",
    "hifi": "pacbio_hifi",
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


def _extract_non_empty_file(entry: Dict[str, Any]) -> Optional[str]:
    """Extract a usable read path from legacy read library objects."""
    for key in ("file", "read", "read1", "read_file", "reads_file", "path"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _normalize_legacy_cga_libs(params: Dict[str, Any]) -> tuple[Dict[str, Any], List[str], List[str]]:
    """Normalize legacy CGA read library payload shapes into canonical structure."""
    normalized = dict(params)
    warnings: List[str] = []
    errors: List[str] = []

    paired_libs = normalized.get("paired_end_libs")
    if isinstance(paired_libs, list) and paired_libs:
        # Variant: one object per pair using left_reads/right_reads (or r1/r2).
        converted_pair_objects: List[Dict[str, Any]] = []
        saw_left_right_shape = False
        pair_alias_sets = (
            ("left_reads", "right_reads"),
            ("forward_reads", "reverse_reads"),
            ("forward", "reverse"),
            ("r1", "r2"),
        )
        for entry in paired_libs:
            if not isinstance(entry, dict):
                converted_pair_objects = []
                break
            read1 = entry.get("read1")
            read2 = entry.get("read2")
            if isinstance(read1, str) and read1.strip():
                converted_pair_objects.append(entry)
                continue
            left = None
            right = None
            for left_key, right_key in pair_alias_sets:
                left_candidate = entry.get(left_key)
                right_candidate = entry.get(right_key)
                if (
                    isinstance(left_candidate, str)
                    and left_candidate.strip()
                    and isinstance(right_candidate, str)
                    and right_candidate.strip()
                ):
                    left = left_candidate.strip()
                    right = right_candidate.strip()
                    break
            if left and right:
                saw_left_right_shape = True
                converted_pair_objects.append(
                    {
                        "read1": left,
                        "read2": right,
                        "platform": entry.get("platform", "infer"),
                        "interleaved": bool(entry.get("interleaved", False)),
                        "read_orientation_outward": bool(entry.get("read_orientation_outward", False)),
                    }
                )
            else:
                converted_pair_objects = []
                break
        if saw_left_right_shape and converted_pair_objects:
            normalized["paired_end_libs"] = converted_pair_objects
            warnings.append(
                "paired_end_libs used left_reads/right_reads; normalized to canonical read1/read2 objects."
            )

        has_canonical_pair = any(
            isinstance(item, dict)
            and isinstance(item.get("read1"), str)
            and item.get("read1", "").strip()
            for item in normalized.get("paired_end_libs", [])
        )
        if not has_canonical_pair:
            legacy_files: List[str] = []
            for entry in normalized.get("paired_end_libs", []):
                if isinstance(entry, dict):
                    file_path = _extract_non_empty_file(entry)
                    if file_path:
                        legacy_files.append(file_path)
            if legacy_files:
                if len(legacy_files) % 2 != 0:
                    errors.append(
                        "paired_end_libs legacy format detected (file/lib_type) but file count is odd; "
                        "cannot infer read pairs. Provide canonical paired_end_libs with read1/read2."
                    )
                else:
                    canonical_pairs = []
                    for i in range(0, len(legacy_files), 2):
                        canonical_pairs.append(
                            {
                                "read1": legacy_files[i],
                                "read2": legacy_files[i + 1],
                                "platform": "infer",
                            }
                        )
                    normalized["paired_end_libs"] = canonical_pairs
                    warnings.append(
                        "paired_end_libs used legacy file/lib_type objects; normalized to canonical read1/read2 pairs."
                    )

    single_libs = normalized.get("single_end_libs")
    if isinstance(single_libs, list) and single_libs:
        has_canonical_single = any(
            isinstance(item, dict)
            and isinstance(item.get("read"), str)
            and item.get("read", "").strip()
            for item in single_libs
        )
        if not has_canonical_single:
            canonical_singles = []
            for entry in single_libs:
                if isinstance(entry, dict):
                    file_path = _extract_non_empty_file(entry)
                    if file_path:
                        canonical_singles.append({"read": file_path, "platform": "infer"})
            if canonical_singles:
                normalized["single_end_libs"] = canonical_singles
                warnings.append(
                    "single_end_libs used legacy file/lib_type objects; normalized to canonical read/platform objects."
                )

    return normalized, warnings, errors


def _normalize_platform(value: Optional[str]) -> Optional[str]:
    if value is None:
        return value
    if not isinstance(value, str):
        raise ValueError(f"platform must be a string, got {type(value)}")
    candidate = value.strip().lower()
    canonical = CGA_PLATFORM_ALIASES.get(candidate, candidate)
    if canonical not in CGA_PLATFORMS:
        raise ValueError(f"platform must be one of {sorted(CGA_PLATFORMS)}, got {value!r}")
    return canonical


class PairedEndLib(BaseModel):
    """Model for paired-end library structure."""
    read1: str
    read2: Optional[str] = None
    interleaved: bool = False
    read_orientation_outward: bool = False
    platform: str = "infer"

    class Config:
        extra = "allow"

    @field_validator("platform", mode="before")
    @classmethod
    def validate_platform(cls, value):
        return _normalize_platform(value if value is not None else "infer")

    @field_validator("read1")
    @classmethod
    def validate_read1(cls, value):
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read1 is required and must be a non-empty string")
        return value.strip()

    @field_validator("read2")
    @classmethod
    def validate_read2(cls, value):
        if value is None:
            return value
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read2 must be a non-empty string when provided")
        return value.strip()


class SingleEndLib(BaseModel):
    """Model for single-end library structure."""
    read: str
    platform: str = "infer"

    class Config:
        extra = "allow"

    @field_validator("platform", mode="before")
    @classmethod
    def validate_platform(cls, value):
        return _normalize_platform(value if value is not None else "infer")

    @field_validator("read")
    @classmethod
    def validate_read(cls, value):
        if not isinstance(value, str) or not value.strip():
            raise ValueError("read is required and must be a non-empty string")
        return value.strip()


class ComprehensiveGenomeAnalysisParams(BaseModel):
    """Pydantic model for ComprehensiveGenomeAnalysis step parameters."""
    input_type: str = Field(..., description="Input type")
    output_path: str = Field(..., description="Workspace output path")
    output_file: str = Field(..., description="Output file basename")
    scientific_name: str = Field(..., description="Scientific name")

    paired_end_libs: Optional[List[Dict[str, Any]]] = Field(None, description="Paired-end libraries")
    single_end_libs: Optional[List[Dict[str, Any]]] = Field(None, description="Single-end libraries")
    srr_ids: Optional[List[str]] = Field(None, description="List of SRR IDs")

    reference_assembly: Optional[str] = Field(None, description="Reference assembly contigs")
    contigs: Optional[str] = Field(None, description="Contigs object")
    genbank_file: Optional[str] = Field(None, description="GenBank input object")
    gto: Optional[str] = Field(None, description="Preannotated genome object")

    taxonomy_id: Optional[int] = Field(None, description="NCBI taxonomy ID")
    code: int = Field(0, description="Genetic code")
    domain: str = Field("auto", description="Domain")

    recipe: str = Field("auto", description="Assembly recipe")
    racon_iter: int = Field(2, description="Racon iterations")
    pilon_iter: int = Field(2, description="Pilon iterations")
    trim: bool = Field(False, description="Trim reads")
    normalize: bool = Field(False, description="Normalize reads")
    filtlong: bool = Field(False, description="Filter long reads")
    target_depth: int = Field(200, description="Target depth")
    genome_size: Optional[Any] = Field(5000000, description="Estimated genome size")
    min_contig_len: int = Field(300, description="Min contig length")
    min_contig_cov: float = Field(5.0, description="Min contig coverage")
    public: bool = Field(False, description="Public genome")
    queue_nowait: bool = Field(False, description="Skip waiting for indexing queue")
    skip_indexing: bool = Field(False, description="Do not index")
    analyze_quality: Optional[bool] = Field(None, description="Enable quality analysis")
    debug_level: int = Field(0, description="Debug level")

    # Legacy/frontend convenience fields accepted but normalized by validation logic.
    coverage: Optional[int] = Field(None, description="Legacy alias for target_depth")
    expected_genome_size: Optional[int] = Field(None, description="Legacy estimated genome size")
    genome_size_units: Optional[str] = Field(None, description="Legacy genome size units")

    class Config:
        extra = "allow"

    @field_validator("input_type", mode="before")
    @classmethod
    def validate_input_type(cls, value):
        if not isinstance(value, str):
            raise ValueError("input_type must be a string")
        candidate = value.strip().lower()
        candidate = CGA_INPUT_TYPE_ALIASES.get(candidate, candidate)
        if candidate not in CGA_INPUT_TYPES:
            raise ValueError(f"input_type must be one of {sorted(CGA_INPUT_TYPES)}, got {value!r}")
        return candidate

    @field_validator("output_path", "output_file", "scientific_name")
    @classmethod
    def validate_required_strings(cls, value):
        if not isinstance(value, str) or not value.strip():
            raise ValueError("value must be a non-empty string")
        return value.strip()

    @field_validator("taxonomy_id", mode="before")
    @classmethod
    def validate_taxonomy_id(cls, value):
        if value is None:
            return value
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
        try:
            value = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"taxonomy_id must be an integer, got {value!r}") from exc
        if value <= 0:
            raise ValueError("taxonomy_id must be a positive integer")
        return value

    @field_validator("code", mode="before")
    @classmethod
    def validate_code(cls, value):
        try:
            code = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"code must be one of {sorted(CGA_CODES)}, got {value!r}") from exc
        if code not in CGA_CODES:
            raise ValueError(f"code must be one of {sorted(CGA_CODES)}, got {code!r}")
        return code

    @field_validator("domain", mode="before")
    @classmethod
    def validate_domain(cls, value):
        if not isinstance(value, str):
            raise ValueError("domain must be a string")
        candidate = CGA_DOMAIN_ALIASES.get(value.strip().lower(), value.strip())
        if candidate not in CGA_DOMAINS:
            raise ValueError(f"domain must be one of {sorted(CGA_DOMAINS)}, got {value!r}")
        return candidate

    @field_validator("recipe", mode="before")
    @classmethod
    def validate_recipe(cls, value):
        if value is None:
            return "auto"
        if not isinstance(value, str):
            raise ValueError("recipe must be a string")
        candidate = value.strip().lower()
        candidate = CGA_RECIPE_ALIASES.get(candidate, candidate)
        if candidate not in CGA_RECIPES:
            raise ValueError(f"recipe must be one of {sorted(CGA_RECIPES)}, got {value!r}")
        return candidate

    @field_validator(
        "trim",
        "normalize",
        "filtlong",
        "public",
        "queue_nowait",
        "skip_indexing",
        "analyze_quality",
        mode="before",
    )
    @classmethod
    def validate_booleans(cls, value):
        if value is None:
            return value
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            candidate = value.strip().lower()
            if candidate in {"true", "1", "yes", "on"}:
                return True
            if candidate in {"false", "0", "no", "off"}:
                return False
        raise ValueError(f"value must be boolean-like, got {value!r}")

    @field_validator("racon_iter", "pilon_iter", "target_depth", "min_contig_len", "debug_level", mode="before")
    @classmethod
    def validate_non_negative_ints(cls, value):
        try:
            intval = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"value must be an integer, got {value!r}") from exc
        if intval < 0:
            raise ValueError("value must be >= 0")
        return intval

    @field_validator("min_contig_cov", mode="before")
    @classmethod
    def validate_positive_float(cls, value):
        try:
            floatval = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"min_contig_cov must be numeric, got {value!r}") from exc
        if floatval <= 0:
            raise ValueError("min_contig_cov must be > 0")
        return floatval

    @field_validator("genome_size", mode="before")
    @classmethod
    def validate_genome_size(cls, value):
        if value is None:
            return value
        if isinstance(value, (int, float)):
            if value <= 0:
                raise ValueError("genome_size must be > 0")
            return int(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            if stripped[-1].lower() in {"k", "m", "g"} and stripped[:-1].isdigit():
                return stripped
            if stripped.isdigit():
                return int(stripped)
        raise ValueError("genome_size must be a positive integer or size string like '5M'")

    @field_validator("paired_end_libs", "single_end_libs", "srr_ids")
    @classmethod
    def validate_non_empty_lists(cls, value):
        if value is not None and isinstance(value, list) and len(value) == 0:
            raise ValueError("list cannot be empty")
        return value


class ComprehensiveGenomeAnalysisValidator(BaseStepValidator):
    """Validator for ComprehensiveGenomeAnalysis service steps."""

    def validate_params(
        self,
        params: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        warnings: List[str] = []
        errors: List[str] = []
        validated_params = params.copy()

        # Normalize common legacy aliases before Pydantic validation.
        if "coverage" in validated_params and "target_depth" not in validated_params:
            validated_params["target_depth"] = validated_params.get("coverage")

        # Frontend-derived jobs effectively tie scientific_name to output naming;
        # tolerate blank scientific_name by deriving from output_file.
        sci_name = validated_params.get("scientific_name")
        if not isinstance(sci_name, str) or not sci_name.strip():
            fallback_name = validated_params.get("output_file")
            if isinstance(fallback_name, str) and fallback_name.strip():
                validated_params["scientific_name"] = fallback_name.strip()
                warnings.append(
                    "scientific_name was empty; using output_file as fallback value."
                )

        # Normalize known legacy read-lib shapes before strict model validation.
        validated_params, legacy_warnings, legacy_errors = _normalize_legacy_cga_libs(
            validated_params
        )
        warnings.extend(legacy_warnings)
        errors.extend(legacy_errors)

        try:
            validated_model = ComprehensiveGenomeAnalysisParams(**validated_params)
            validated_params = validated_model.model_dump(exclude_none=False)

            # Validate nested library structure.
            if validated_params.get("paired_end_libs"):
                for i, lib in enumerate(validated_params["paired_end_libs"]):
                    try:
                        model = PairedEndLib(**lib)
                        if not model.interleaved and not model.read2:
                            errors.append(
                                f"paired_end_libs[{i}]: read2 is required when interleaved is false."
                            )
                    except ValidationError as e:
                        errors.append(f"paired_end_libs[{i}]: {e.errors()[0]['msg']}")

            if validated_params.get("single_end_libs"):
                for i, lib in enumerate(validated_params["single_end_libs"]):
                    try:
                        SingleEndLib(**lib)
                    except ValidationError as e:
                        errors.append(f"single_end_libs[{i}]: {e.errors()[0]['msg']}")

            # Conditional required and compatibility checks by input_type.
            input_type = validated_params.get("input_type")
            has_reads = any(
                validated_params.get(field) not in (None, "", [])
                for field in ("paired_end_libs", "single_end_libs", "srr_ids")
            )
            has_contigs = any(
                validated_params.get(field) not in (None, "", [])
                for field in ("contigs", "reference_assembly")
            )
            has_genbank = any(
                validated_params.get(field) not in (None, "", [])
                for field in ("genbank_file", "gto")
            )

            if input_type == "reads":
                if not has_reads:
                    errors.append(
                        "When input_type is 'reads', provide at least one of: paired_end_libs, single_end_libs, srr_ids."
                    )
                if has_contigs or has_genbank:
                    errors.append(
                        "When input_type is 'reads', do not provide contigs/genbank inputs (contigs, reference_assembly, genbank_file, gto)."
                    )
            elif input_type == "contigs":
                if not has_contigs:
                    errors.append(
                        "When input_type is 'contigs', provide at least one of: contigs, reference_assembly."
                    )
                if has_reads or has_genbank:
                    errors.append(
                        "When input_type is 'contigs', do not provide reads/genbank inputs."
                    )
            elif input_type == "genbank":
                if not has_genbank:
                    errors.append(
                        "When input_type is 'genbank', provide at least one of: genbank_file, gto."
                    )
                if has_reads or has_contigs:
                    errors.append(
                        "When input_type is 'genbank', do not provide reads/contigs inputs."
                    )

            # Output-path style warning mirrors other validators.
            output_path = validated_params.get("output_path", "")
            if output_path and not output_path.startswith("${") and not output_path.startswith("/"):
                warnings.append(
                    f"output_path '{output_path}' does not appear absolute (expected '/...' or variable reference)."
                )

            if validated_params.get("taxonomy_id") is None:
                warnings.append(
                    "taxonomy_id is not provided; job may still run but taxonomy-driven validation is weaker."
                )

            logger.debug(
                "ComprehensiveGenomeAnalysis: validated %s parameters",
                len(validated_params),
            )

        except ValidationError as e:
            for error in e.errors():
                field = ".".join(str(loc) for loc in error["loc"])
                errors.append(f"Parameter '{field}': {error['msg']}")
            logger.warning(
                "ComprehensiveGenomeAnalysis validation failed with %s error(s)",
                len(errors),
            )
        except Exception as e:
            errors.append(f"Unexpected validation error: {str(e)}")
            logger.error(
                "ComprehensiveGenomeAnalysis unexpected validation error: %s",
                e,
                exc_info=True,
            )

        return ValidationResult(params=validated_params, warnings=warnings, errors=errors)

    def validate_outputs(
        self,
        outputs: Dict[str, str],
        params: Dict[str, Any],
        app_name: str
    ) -> ValidationResult:
        warnings: List[str] = []
        errors: List[str] = []

        parent_result = super().validate_outputs(outputs, params, app_name)
        warnings.extend(parent_result.warnings)
        errors.extend(parent_result.errors)

        expected_outputs = [
            "genome_object",
            "contigs_fasta",
            "annotation_report",
            "genbank_file",
            "gff_file",
            "genome_report",
            "genome_file",
            "job_output_path",
        ]

        for key, output_value in outputs.items():
            if key not in expected_outputs:
                warnings.append(
                    f"Output key '{key}' is not a standard ComprehensiveGenomeAnalysis output. "
                    f"Expected keys: {', '.join(expected_outputs)}"
                )
            if (
                key in expected_outputs
                and
                isinstance(output_value, str)
                and "${params.output_path}" not in output_value
                and "${params.output_file}" not in output_value
                and not output_value.startswith("${")
                and not output_value.startswith("/")
            ):
                warnings.append(
                    f"Output '{key}' path does not reference params.output_path/output_file."
                )

        return ValidationResult(params={}, warnings=warnings, errors=errors)

