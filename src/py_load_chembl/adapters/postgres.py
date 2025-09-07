import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

import psycopg2
from py_load_chembl.adapters.base import DatabaseAdapter


class PostgresAdapter(DatabaseAdapter):
    """Database adapter for PostgreSQL."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.conn = None
        self._parsed_uri = urlparse(self.connection_string)

    def connect(self) -> Any:
        """Establishes a connection to the target database."""
        if self.conn is None or self.conn.closed:
            try:
                self.conn = psycopg2.connect(self.connection_string)
            except psycopg2.OperationalError as e:
                raise ConnectionError(f"Failed to connect to PostgreSQL: {e}") from e
        return self.conn

    def execute_ddl(self, ddl_script: str) -> None:
        """Executes a DDL script (e.g., schema creation, index management)."""
        # This might be used for schema creation if not using pg_restore
        pass

    def bulk_load_table(
        self, table_name: str, data_source: Path | str, schema: str = "public", options: Dict[str, Any] = {}
    ) -> None:
        """
        Performs a high-performance native bulk load using pg_restore.

        Note: For a full database dump, table_name and schema are ignored
        as pg_restore restores the entire database defined in the dump.
        """
        print(f"Initiating full database restore from '{data_source}'...")
        self._run_pg_restore(Path(data_source))
        print("Database restore completed.")

    def execute_merge(self, source_table: str, target_table: str, primary_keys: List[str], columns: List[str]) -> None:
        """
        Executes an efficient MERGE/UPSERT operation from source (staging) to target (production).
        """
        pass

    def optimize_pre_load(self, schema: str) -> None:
        """Disables constraints and indexes before a full load."""
        # pg_restore handles this by default (data is loaded first, then indexes/constraints are created)
        pass

    def optimize_post_load(self, schema: str) -> None:
        """Re-enables constraints, rebuilds indexes, and runs statistics gathering (e.g., ANALYZE)."""
        print("Optimizing database post-load: Running ANALYZE...")
        conn = self.connect()
        # Autocommit mode to run ANALYZE outside a transaction block
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with conn.cursor() as cursor:
                cursor.execute("ANALYZE VERBOSE;")
            print("ANALYZE command completed successfully.")
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to run ANALYZE on the database: {e}") from e
        finally:
            # It's good practice to reset the isolation level
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_DEFAULT)


    def _run_pg_restore(self, dump_archive_path: Path):
        """Extracts the dump and runs the pg_restore command."""
        if not shutil.which("pg_restore"):
            raise FileNotFoundError(
                "'pg_restore' command not found. Please ensure the PostgreSQL client tools are installed and in your system's PATH."
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            print(f"Extracting '{dump_archive_path.name}' to a temporary directory...")
            try:
                with tarfile.open(dump_archive_path, "r:gz") as tar:
                    tar.extractall(path=tmpdir)
            except tarfile.TarError as e:
                raise IOError(f"Failed to extract tar.gz file: {e}") from e

            # The archive contains a single directory, e.g., 'chembl_33_postgresql'
            # This directory contains the actual dump files for pg_restore.
            extracted_dirs = [d for d in Path(tmpdir).iterdir() if d.is_dir()]
            if not extracted_dirs:
                raise FileNotFoundError("No directory found in the extracted ChEMBL archive.")
            dump_dir = extracted_dirs[0]

            # Use a high number of jobs for parallel processing, let pg_restore manage it.
            # FRD recommends using -j flag. os.cpu_count() is a sensible default.
            jobs = os.cpu_count() or 4

            command = [
                "pg_restore",
                "--verbose",
                "--exit-on-error",
                f"--dbname={self._parsed_uri.path.lstrip('/')}",
                f"--jobs={jobs}",
                "--no-owner",      # Often desirable when restoring to a different system
                "--no-privileges", # ACLs are often environment-specific
                str(dump_dir),
            ]

            env = os.environ.copy()
            env["PGHOST"] = self._parsed_uri.hostname or "localhost"
            env["PGPORT"] = str(self._parsed_uri.port or 5432)
            env["PGUSER"] = self._parsed_uri.username or ""
            if self._parsed_uri.password:
                env["PGPASSWORD"] = self._parsed_uri.password

            print(f"Running pg_restore with {jobs} parallel jobs...")
            try:
                process = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    check=True, # Raises CalledProcessError on non-zero exit codes
                    env=env,
                )
                print("--- pg_restore STDOUT ---")
                print(process.stdout)
                if process.stderr:
                    print("--- pg_restore STDERR ---")
                    print(process.stderr)

            except FileNotFoundError:
                raise RuntimeError(
                    f"pg_restore command not found. Make sure PostgreSQL client tools are installed and in your PATH."
                )
            except subprocess.CalledProcessError as e:
                error_message = (
                    f"pg_restore failed with exit code {e.returncode}.\n"
                    f"Ensure the target database '{self._parsed_uri.path.lstrip('/')}' exists and is empty.\n"
                    f"--- STDOUT ---\n{e.stdout}\n"
                    f"--- STDERR ---\n{e.stderr}"
                )
                raise RuntimeError(error_message) from e
