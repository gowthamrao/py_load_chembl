from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pathlib import Path


class DatabaseAdapter(ABC):
    @abstractmethod
    def connect(self) -> Any:
        """Establishes a connection to the target database."""
        pass

    @abstractmethod
    def execute_ddl(self, ddl_script: str) -> None:
        """Executes a DDL script (e.g., schema creation, index management)."""
        pass

    @abstractmethod
    def bulk_load_table(
        self,
        table_name: str,
        data_source: Path | str,
        schema: str = "public",
        options: Dict[str, Any] = {},
    ) -> None:
        """
        Performs high-performance native bulk loading into a specific table.
        Args:
            table_name: The target table.
            data_source: Path (local or cloud URI) to the intermediate file or dump segment.
            schema: The target schema (e.g., 'public', 'staging').
        """
        pass

    @abstractmethod
    def execute_sql(
        self, sql: str, params: Optional[tuple] = None, fetch: Optional[str] = None
    ) -> Any:
        """Executes an arbitrary SQL command."""
        pass

    @abstractmethod
    def create_metadata_tables(self) -> None:
        """Creates the metadata tracking tables if they don't exist."""
        pass

    @abstractmethod
    def clean_schema(self, schema: str) -> None:
        """Drops and recreates a schema."""
        pass

    @abstractmethod
    def get_table_names(self, schema: str) -> List[str]:
        """Returns a list of table names in a given schema."""
        pass

    @abstractmethod
    def get_column_names(self, schema: str, table_name: str) -> List[str]:
        """Returns a list of column names for a given table in a schema."""
        pass

    @abstractmethod
    def execute_merge(
        self,
        source_table: str,
        target_table: str,
        primary_keys: List[str],
        all_columns: List[str],
    ) -> Dict[str, int]:
        """
        Executes an efficient MERGE/UPSERT operation from source (staging) to target (production).
        """
        pass

    @abstractmethod
    def handle_obsolete_records(self, source_schema: str, target_schema: str) -> int:
        """
        Handles obsolete records based on the chembl_id_lookup table.

        This method should compare the source and target chembl_id_lookup tables
        and update the status of any entities that have become obsolete in the
        new ChEMBL version.

        Returns:
            The number of records marked as obsolete.
        """
        pass

    @abstractmethod
    def optimize_pre_load(self, schema: str) -> None:
        """Disables constraints and indexes before a full load."""
        pass

    @abstractmethod
    def optimize_post_load(self, schema: str) -> None:
        """Re-enables constraints, rebuilds indexes, and runs statistics gathering (e.g., ANALYZE)."""
        pass

    @abstractmethod
    def get_column_definitions(
        self, schema: str, table_name: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieves the column definitions for a given table.

        Returns:
            A list of dictionaries, where each dictionary represents a column
            and contains keys like 'name', 'type', 'length'.
        """
        pass
