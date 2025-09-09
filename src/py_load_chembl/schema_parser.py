import gzip
import re
import logging
from collections import namedtuple
from pathlib import Path
from typing import Dict

TableSchema = namedtuple("TableSchema", ["name", "primary_keys", "columns"])
logger = logging.getLogger(__name__)

def parse_chembl_ddl(ddl_path: Path) -> Dict[str, TableSchema]:
    """
    Parses a ChEMBL PostgreSQL DDL file to extract table names and their primary keys.
    This parser uses a regular expression to find 'ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY'
    statements, which is the standard way ChEMBL defines its primary keys.

    Args:
        ddl_path: Path to the compressed ChEMBL DDL file.

    Returns:
        A dictionary mapping table names to TableSchema namedtuples.
    """
    logger.info(f"Parsing ChEMBL DDL from: {ddl_path}...")
    schemas: Dict[str, TableSchema] = {}

    try:
        with gzip.open(ddl_path, 'rt', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        raise IOError(f"Failed to read or decompress DDL file at {ddl_path}") from e

    # This regex is designed to find all 'ALTER TABLE' statements that add a primary key.
    # It captures the table name and the list of columns in the primary key.
    pattern = re.compile(
        r"ALTER TABLE(?: ONLY)?\s+([\w\.\"_]+)\s+ADD CONSTRAINT.*?PRIMARY KEY\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL
    )

    matches = pattern.finditer(content)

    for match in matches:
        table_name = match.group(1).strip().strip('"')
        pk_columns = [col.strip().strip('"') for col in match.group(2).split(',')]

        if table_name and pk_columns:
            if table_name not in schemas:
                logger.debug(f"Found PK for table '{table_name}': {pk_columns}")
                schemas[table_name] = TableSchema(name=table_name, primary_keys=pk_columns, columns=[])

    if not schemas:
        logger.warning("DDL parser did not find any primary key constraints using regex.")
    else:
        logger.info(f"Successfully parsed {len(schemas)} primary key definitions.")

    return schemas
