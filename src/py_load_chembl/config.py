# -*- coding: utf-8 -*-

"""
Configuration for py_load_chembl, including predefined lists of tables.
"""
import enum

class Representation(str, enum.Enum):
    """Enum for the different data representations that can be loaded."""
    FULL = "full"
    STANDARD = "standard"


# A list of core ChEMBL tables for the "standard" representation.
# This set is designed to be self-consistent and support common
# Structure-Activity Relationship (SAR) analysis use cases.
# It includes the core entities (molecules, targets, assays, activities)
# and their essential supporting lookup tables.
STANDARD_TABLE_SUBSET = sorted([
    # Core Data Tables
    "activities",
    "assays",
    "molecule_dictionary",
    "compound_structures",
    "target_dictionary",

    # Essential Supporting Tables
    "chembl_id_lookup",
    "compound_properties",
    "source",
    "target_type",
    "organism_class",

    # Foreign key dependencies for the above tables to ensure integrity
    "activity_stds_lookup",
    "assay_type",
    "component_sequences",
    "confidence_score_lookup",
    "compound_records",
    "data_validity_lookup",
    "docs",
    "relationship_type",
    "research_stem",
    "site_components",
    "cell_dictionary",
    "protein_family_classification",
    "protein_classification",
])
