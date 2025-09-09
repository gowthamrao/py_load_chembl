import pytest
from pathlib import Path
from py_load_chembl.schema_parser import parse_chembl_ddl, TableSchema

# A sample DDL string that mimics the structure of the ChEMBL DDL file.
# It includes comments, other statement types, and the key ALTER TABLE statements.
SAMPLE_DDL = """
-- Some comment here
CREATE TABLE molecule_dictionary (
    molregno bigint NOT NULL,
    chembl_id character varying(20) NOT NULL
);

CREATE TABLE compound_structures (
    molregno bigint NOT NULL,
    canonical_smiles text
);

CREATE INDEX molecule_chembl_id_idx ON molecule_dictionary USING btree (chembl_id);

-- Adding the primary keys using ALTER TABLE, as ChEMBL does.
ALTER TABLE ONLY molecule_dictionary
    ADD CONSTRAINT molecule_dictionary_pkey PRIMARY KEY (molregno);

ALTER TABLE ONLY compound_structures
    ADD CONSTRAINT compound_structures_pkey PRIMARY KEY (molregno);

-- A table with a multi-column primary key
CREATE TABLE assay_type (
    assay_type character varying(1) NOT NULL,
    assay_desc character varying(250)
);

ALTER TABLE ONLY assay_type
    ADD CONSTRAINT assay_type_pkey PRIMARY KEY (assay_type, assay_desc);
"""

@pytest.fixture
def ddl_file(tmp_path: Path) -> Path:
    """Creates a dummy gzipped DDL file for testing."""
    import gzip
    file_path = tmp_path / "test.sql.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_DDL)
    return file_path


def test_parse_chembl_ddl(ddl_file: Path):
    """
    Tests that the DDL parser correctly extracts primary keys from a sample DDL file.
    """
    # Act
    schemas = parse_chembl_ddl(ddl_file)

    # Assert
    assert isinstance(schemas, dict)
    assert len(schemas) == 3

    # Check molecule_dictionary
    assert "molecule_dictionary" in schemas
    mol_dict_schema = schemas["molecule_dictionary"]
    assert isinstance(mol_dict_schema, TableSchema)
    assert mol_dict_schema.name == "molecule_dictionary"
    assert mol_dict_schema.primary_keys == ["molregno"]

    # Check compound_structures
    assert "compound_structures" in schemas
    comp_struct_schema = schemas["compound_structures"]
    assert comp_struct_schema.primary_keys == ["molregno"]

    # Check assay_type (multi-column PK)
    assert "assay_type" in schemas
    assay_type_schema = schemas["assay_type"]
    assert assay_type_schema.primary_keys == ["assay_type", "assay_desc"]

def test_parse_empty_ddl(tmp_path: Path):
    """
    Tests that the parser handles an empty DDL file gracefully.
    """
    # Arrange
    import gzip
    file_path = tmp_path / "empty.sql.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write("")

    # Act
    schemas = parse_chembl_ddl(file_path)

    # Assert
    assert schemas == {}
