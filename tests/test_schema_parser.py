import pytest
from py_load_chembl.schema_parser import parse_chembl_ddl, TableSchema

# A sample DDL with a simple foreign key relationship for testing
# target_dictionary must come before assays
SAMPLE_DDL = """
-- Table Definition
CREATE TABLE "target_dictionary" (
    "tid" numeric NOT NULL,
    "pref_name" varchar(255) NOT NULL,
    "target_type" varchar(30),
    "chembl_id" varchar(20) NOT NULL,
    PRIMARY KEY("tid")
);

-- Another table
CREATE TABLE "assays" (
    "assay_id" numeric NOT NULL,
    "tid" numeric,
    "description" varchar(4000),
    "chembl_id" varchar(20) NOT NULL,
    PRIMARY KEY("assay_id", "chembl_id")
);

-- Add a foreign key constraint
ALTER TABLE "assays" ADD CONSTRAINT "assays_tid_fk" FOREIGN KEY ("tid") REFERENCES "target_dictionary"("tid");

-- Some other statement to be ignored
CREATE INDEX assays_chembl_id_idx ON assays(chembl_id);
"""

def test_parse_ddl_finds_all_tables():
    """Tests if the parser identifies the correct number of tables."""
    schemas = parse_chembl_ddl(SAMPLE_DDL)
    assert len(schemas) == 2
    assert {s.name for s in schemas} == {"target_dictionary", "assays"}

def test_topological_sort_order():
    """Tests if the tables are returned in the correct dependency order."""
    schemas = parse_chembl_ddl(SAMPLE_DDL)
    table_names = [s.name for s in schemas]
    # target_dictionary must come before assays because assays depends on it
    assert table_names.index("target_dictionary") < table_names.index("assays")

def test_parse_columns_and_types():
    """Tests if columns and their data types are parsed correctly."""
    schemas = parse_chembl_ddl(SAMPLE_DDL)
    assays_schema = next(s for s in schemas if s.name == "assays")

    assert len(assays_schema.columns) == 4

    # Check a specific column
    tid_column = next(c for c in assays_schema.columns if c.name == "tid")
    assert tid_column.dtype == "numeric"

    desc_column = next(c for c in assays_schema.columns if c.name == "description")
    assert desc_column.dtype == "varchar(4000)"

def test_parse_primary_keys():
    """Tests if single and composite primary keys are parsed correctly."""
    schemas = parse_chembl_ddl(SAMPLE_DDL)

    # Single PK
    target_schema = next(s for s in schemas if s.name == "target_dictionary")
    assert target_schema.primary_keys == ["tid"]

    # Composite PK
    assays_schema = next(s for s in schemas if s.name == "assays")
    assert sorted(assays_schema.primary_keys) == sorted(["assay_id", "chembl_id"])

def test_parse_dependencies():
    """Tests if foreign key dependencies are correctly identified."""
    schemas = parse_chembl_ddl(SAMPLE_DDL)
    assays_schema = next(s for s in schemas if s.name == "assays")
    target_schema = next(s for s in schemas if s.name == "target_dictionary")

    assert "target_dictionary" in assays_schema.dependencies
    assert len(target_schema.dependencies) == 0
