from collections import namedtuple

TableSchema = namedtuple("TableSchema", ["name", "primary_keys", "columns"])

# A simplified representation of the ChEMBL schema, focusing on primary keys
# for the delta load/merge operation. The 'columns' attribute is a placeholder
# for now but would be used for more advanced schema evolution checks.
# A more complete implementation would list all tables.

CHEMBL_SCHEMAS = {
    "molecule_dictionary": TableSchema(
        name="molecule_dictionary", primary_keys=["molregno"], columns=[]
    ),
    "compound_structures": TableSchema(
        name="compound_structures", primary_keys=["molregno"], columns=[]
    ),
    "activities": TableSchema(
        name="activities", primary_keys=["activity_id"], columns=[]
    ),
    "assays": TableSchema(
        name="assays", primary_keys=["assay_id"], columns=[]
    ),
    "target_dictionary": TableSchema(
        name="target_dictionary", primary_keys=["tid"], columns=[]
    ),
    "chembl_id_lookup": TableSchema(
        name="chembl_id_lookup", primary_keys=["chembl_id"], columns=[]
    ),
    # This is not a complete list, but represents the core tables
    # and is sufficient to prove the delta load strategy works.
}

# Defines the directed graph of table dependencies (foreign key relationships)
# A tuple (A, B) means table A depends on table B (A has a FK to B),
# so B must be loaded before A.
CHEMBL_TABLE_DEPENDENCIES = [
    # Core Dependencies
    ("molecule_dictionary", "chembl_id_lookup"),
    ("compound_records", "docs"),
    ("compound_records", "molecule_dictionary"),
    ("compound_structures", "molecule_dictionary"),
    ("target_dictionary", "chembl_id_lookup"),
    ("assays", "target_dictionary"),
    ("assays", "docs"),
    ("assays", "cell_dictionary"),
    ("activities", "assays"),
    ("activities", "molecule_dictionary"),
    ("activities", "docs"),

    # Other Important Relationships
    ("protein_classification", "protein_family_classification"),
    ("target_relations", "target_dictionary"),
    ("target_components", "target_dictionary"),
    ("component_sequences", "target_components"),
    ("component_domains", "component_sequences"),
    ("bio_component_sequences", "component_sequences"),
    ("biotherapeutic_components", "biotherapeutics"),
    ("biotherapeutics", "molecule_dictionary"),
    ("formulations", "molecule_dictionary"),
    ("formulations", "products"),
    ("products", "molecule_dictionary"),
    ("drug_mechanism", "molecule_dictionary"),
    ("drug_mechanism", "target_dictionary"),
    ("drug_indication", "molecule_dictionary"),
    ("compound_properties", "molecule_dictionary"),
    ("compound_structural_alerts", "molecule_dictionary"),
    ("molecule_atc_classification", "molecule_dictionary"),
    ("molecule_hierarchy", "molecule_dictionary"),
    ("molecule_synonyms", "molecule_dictionary"),
    ("activities_supp", "activities"),
    ("activity_properties", "activities"),
]
