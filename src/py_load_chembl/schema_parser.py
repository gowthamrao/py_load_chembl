import re
import sqlparse
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from typing import Dict, List, Set, Tuple


@dataclass
class ColumnInfo:
    """Represents a single column in a database table."""
    name: str
    dtype: str


@dataclass
class TableSchema:
    """Represents the schema of a database table, including its dependencies."""
    name: str
    columns: List[ColumnInfo]
    primary_keys: List[str] = field(default_factory=list)
    dependencies: Set[str] = field(default_factory=set)


def _clean_token(token_value: str) -> str:
    """Removes quotes and schema prefixes from a token's value."""
    return token_value.strip().replace('"', "").replace("`", "").split(".")[-1]


def _parse_create_table(statement: sqlparse.sql.Statement) -> TableSchema | None:
    """Parses a CREATE TABLE statement and returns a TableSchema object."""
    try:
        # token_next_by returns a tuple (index, token)
        table_name_token = statement.token_next_by(i=sqlparse.sql.Identifier)[1]
    except (TypeError, IndexError):
        return None
    table_name = _clean_token(table_name_token.get_real_name())

    columns = []
    primary_keys = []

    # Find the parenthesis block containing column definitions
    try:
        paren = statement.token_next_by(i=sqlparse.sql.Parenthesis)[1]
    except (TypeError, IndexError):
        return None

    # Get the content within the parenthesis and split by comma and newline
    content = str(paren).strip()[1:-1]
    definitions = [item.strip() for item in content.split(',\n')]

    for definition in definitions:
        definition = definition.strip().removesuffix(',')
        if not definition:
            continue

        # Check for PRIMARY KEY constraint
        if definition.upper().startswith("PRIMARY KEY"):
            pk_match = re.search(r"\((.*?)\)", definition, re.IGNORECASE)
            if pk_match:
                primary_keys.extend([_clean_token(k) for k in pk_match.group(1).split(',')])
            continue

        if definition.upper().startswith("CONSTRAINT"):
            # Handle other constraints if necessary, for now we just care about PK
            if "PRIMARY KEY" in definition.upper():
                 pk_match = re.search(r"PRIMARY KEY\s*\((.*?)\)", definition, re.IGNORECASE)
                 if pk_match:
                    primary_keys.extend([_clean_token(k) for k in pk_match.group(1).split(',')])
            continue

        # Otherwise, assume it's a column definition
        parts = definition.split()
        col_name = _clean_token(parts[0])
        col_dtype = " ".join(parts[1:])
        # Remove trailing comma from data type if it exists
        if col_dtype.endswith(','):
            col_dtype = col_dtype[:-1]

        columns.append(ColumnInfo(name=col_name, dtype=col_dtype))

    return TableSchema(name=table_name, columns=columns, primary_keys=primary_keys)


def _parse_alter_table(statement: sqlparse.sql.Statement, schemas: Dict[str, TableSchema]):
    """Parses an ALTER TABLE statement to find foreign key dependencies."""
    sql_str = str(statement).upper()
    if "ADD CONSTRAINT" not in sql_str or "FOREIGN KEY" not in sql_str or "REFERENCES" not in sql_str:
        return

    try:
        # Find the 'TABLE' keyword token
        table_keyword_idx, _ = statement.token_next_by(m=(sqlparse.tokens.Keyword, 'TABLE'))
        # The next token should be the identifier for the table name
        _, table_identifier_token = statement.token_next_by(i=sqlparse.sql.Identifier, idx=table_keyword_idx)

        table_name = _clean_token(table_identifier_token.get_real_name())
    except (TypeError, IndexError, AttributeError):
        # Could not parse table name from statement, skip
        return

    # Find the referenced table
    ref_match = re.search(r"REFERENCES\s+([\w\.\"_`]+)", str(statement), re.IGNORECASE)
    if ref_match:
        referenced_table = _clean_token(ref_match.group(1))
        if table_name in schemas and referenced_table in schemas:
            schemas[table_name].dependencies.add(referenced_table)


def parse_chembl_ddl(ddl_content: str) -> List[TableSchema]:
    """
    Parses a ChEMBL DDL script to extract table schemas and their dependencies,
    and returns them in a topologically sorted order.

    Args:
        ddl_content: A string containing the full SQL DDL script.

    Returns:
        A list of TableSchema objects, sorted in a valid processing order
        based on foreign key dependencies.
    """
    schemas: Dict[str, TableSchema] = {}

    # First pass: Parse all CREATE TABLE statements to populate initial schemas
    for statement in sqlparse.parse(ddl_content):
        if statement.get_type() != "CREATE":
            continue

        # Check if 'TABLE' keyword is present, distinguishing from CREATE INDEX etc.
        is_create_table = any(
            token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'TABLE'
            for token in statement.tokens
        )

        if is_create_table:
            schema = _parse_create_table(statement)
            if schema:
                schemas[schema.name] = schema

    # Second pass: Parse all statements to find dependencies. We don't filter by
    # statement type here, as comments can sometimes affect parsing. The helper
    # function has its own guards.
    for statement in sqlparse.parse(ddl_content):
        _parse_alter_table(statement, schemas)

    # Prepare graph for topological sort
    graph = {name: schema.dependencies for name, schema in schemas.items()}

    # Perform topological sort
    ts = TopologicalSorter(graph)
    try:
        sorted_table_names = list(ts.static_order())
    except Exception as e:
        # Could be a circular dependency
        raise ValueError(f"Failed to sort tables topologically. Is there a circular dependency? Details: {e}") from e

    # Return the full schema objects in the sorted order
    sorted_schemas = [schemas[name] for name in sorted_table_names]
    return sorted_schemas
