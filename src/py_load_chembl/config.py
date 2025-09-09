"""
Configuration values and constants for the py-load-chembl package.
"""
from enum import Enum


class Representation(str, Enum):
    """
    Defines the data representation to load.
    FULL: All tables in the ChEMBL dump.
    STANDARD: A curated subset of the most commonly used tables.
    """
    FULL = "full"
    STANDARD = "standard"


# As defined in FRD 1.3, the "Standard Representation" is a core subset of
# essential tables for common use cases.
STANDARD_TABLE_SUBSET = [
    "molecule_dictionary",
    "compound_structures",
    "activities",
    "assays",
    "target_dictionary",
    "target_components",
    "protein_classification",
    "chembl_id_lookup",
    "source",
    "docs",
]
