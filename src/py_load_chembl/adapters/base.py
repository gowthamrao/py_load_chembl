from abc import ABC, abstractmethod
from typing import Any, Dict, List
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
    def bulk_load_table(self, table_name: str, data_source: Path | str, schema: str = "public", options: Dict[str, Any] = {}) -> None:
        """
        Performs high-performance native bulk loading into a specific table.
        Args:
            table_name: The target table.
            data_source: Path (local or cloud URI) to the intermediate file or dump segment.
            schema: The target schema (e.g., 'public', 'staging').
        """
        pass

    @abstractmethod
    def execute_merge(self, source_table: str, target_table: str, primary_keys: List[str], all_columns: List[str]) -> Dict[str, int]:
        """
        Executes an efficient MERGE/UPSERT operation from source (staging) to target (production).
        """
        pass

    @abstractmethod
    def migrate_schema(self, source_schema: str, source_table_name: str, target_schema: str, target_table_name: str) -> None:
        """
        Compares the schema of a source and target table and applies additive changes
        (e.g., new columns) to the target table.
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
