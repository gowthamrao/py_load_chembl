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
