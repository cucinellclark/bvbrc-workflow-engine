"""
ComprehensiveGenomeAnalysis service default parameter provider.

This module provides default parameters for ComprehensiveGenomeAnalysis service steps.
"""
from typing import Dict, Any
import logging

from .base_defaults import BaseDefaults

logger = logging.getLogger(__name__)


class ComprehensiveGenomeAnalysisDefaults(BaseDefaults):
    """
    Default parameter provider for ComprehensiveGenomeAnalysis service steps.
    
    Provides sensible default values for optional ComprehensiveGenomeAnalysis parameters.
    """
    
    def get_default_params(self, app_name: str) -> Dict[str, Any]:
        """
        Get default parameters for ComprehensiveGenomeAnalysis.
        
        Args:
            app_name: Application name (should be "ComprehensiveGenomeAnalysis")
        
        Returns:
            Dictionary of default parameter values
        """
        return {
            "genome_size": 5000000,
            "normalize": True,
            "trim": True,
            "coverage": 200,
            "expected_genome_size": 5,
            "genome_size_units": "M",
            "racon_iter": 2,
            "pilon_iter": 2,
            "min_contig_len": 300,
            "min_contig_cov": 5,
            "filtlong": True,
            "target_depth": 200,
        }

