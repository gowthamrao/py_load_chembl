import os
import shutil
import subprocess
import tarfile
import tempfile
import gzip
import logging
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

import psycopg2
from py_load_chembl.adapters.base import DatabaseAdapter

logger = logging.getLogger(__name__)


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

    def execute_sql(self, sql: str, params: tuple = None, fetch: str | None = None) -> Any:
        """Executes an arbitrary SQL command."""
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                if fetch == 'one':
                    return cursor.fetchone()
                if fetch == 'all':
                    return cursor.fetchall()
                conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            raise RuntimeError(f"Database query failed: {e}") from e

    def create_metadata_tables(self) -> None:
        """Creates the metadata tracking tables if they don't exist."""
        logger.info("Ensuring metadata tables exist...")
        meta_schema = "chembl_loader_meta"
        schema_ddl = f"CREATE SCHEMA IF NOT EXISTS {meta_schema};"

        history_ddl = f"""
        CREATE TABLE IF NOT EXISTS {meta_schema}.load_history (
            load_id SERIAL PRIMARY KEY,
            chembl_version VARCHAR(10) NOT NULL,
            load_type VARCHAR(10) NOT NULL,
            start_timestamp TIMESTAMPTZ NOT NULL,
            end_timestamp TIMESTAMPTZ,
            status VARCHAR(10) NOT NULL,
            loader_version VARCHAR(20),
            error_message TEXT
        );
        """

        details_ddl = f"""
        CREATE TABLE IF NOT EXISTS {meta_schema}.load_details (
            detail_id SERIAL PRIMARY KEY,
            load_id INTEGER NOT NULL REFERENCES {meta_schema}.load_history(load_id),
            table_name VARCHAR(100) NOT NULL,
            stage_record_count BIGINT,
            insert_count BIGINT,
            update_count BIGINT,
            obsolete_count BIGINT
        );
        """
        self.execute_sql(schema_ddl)
        self.execute_sql(history_ddl)
        self.execute_sql(details_ddl)
        logger.info("Metadata tables are ready.")

    def execute_ddl(self, ddl_script: str) -> None:
        """Executes a DDL script (e.g., schema creation, index management)."""
        logger.info("Executing DDL...")
        self.execute_sql(ddl_script)
        logger.info("DDL execution complete.")

    def bulk_load_table(
        self, table_name: str, data_source: Path | str, schema: str = "public", options: Dict[str, Any] = {}
    ) -> None:
        """
        Performs high-performance native bulk loading.
        - If data_source is a .sql.gz archive, uses psql for a schema-specific restore.
        - If data_source is a .tar.gz archive, uses pg_restore for a full restore.
        - If data_source is a .tsv file, uses the COPY command for a single table.
        """
        data_source_path = Path(data_source)
        if data_source_path.name.endswith(".sql.gz"):
            logger.info(f"Initiating schema-specific restore from '{data_source_path}' using psql...")
            self._run_psql_restore(data_source_path, schema=schema)
            logger.info("Schema-specific restore completed.")
        elif data_source_path.name.endswith(".tar.gz"):
            logger.info(f"Initiating full database restore from '{data_source_path}' into schema '{schema}'...")
            include_tables = options.get("include_tables")
            self._run_pg_restore(data_source_path, schema=schema, table_list=include_tables)
            logger.info("Database restore completed.")
        elif data_source_path.name.endswith(".tsv"):
            logger.info(f"Initiating COPY load into '{schema}.{table_name}' from '{data_source_path}'...")
            conn = self.connect()
            sql = f"COPY {schema}.{table_name} FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL 'None')"
            try:
                with conn.cursor() as cursor, open(data_source_path, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(sql, f)
                conn.commit()
                logger.info("COPY load completed.")
            except psycopg2.Error as e:
                conn.rollback()
                raise RuntimeError(f"Failed to COPY data into {schema}.{table_name}: {e}") from e
        else:
            raise ValueError(f"Unsupported data source format: {data_source_path.name}")

    def execute_merge(self, source_table: str, target_table: str, primary_keys: List[str], all_columns: List[str]) -> Dict[str, int]:
        """
        Executes an efficient MERGE/UPSERT operation from source (staging) to target (production)
        using INSERT ... ON CONFLICT. This method is robust to schema differences where the
        target table may have extra columns (like 'is_deleted').

        Returns a dictionary with 'inserted' and 'updated' counts.
        """
        logger.info(f"Merging data from '{source_table}' into '{target_table}'...")

        # Introspect columns to handle schema differences gracefully
        source_schema, source_table_name = source_table.split('.')
        target_schema, target_table_name = target_table.split('.')

        source_cols = self.get_column_names(source_schema.replace('"', ''), source_table_name.replace('"', ''))
        target_cols = self.get_column_names(target_schema.replace('"', ''), target_table_name.replace('"', ''))

        # We can only insert and update columns that exist in both tables
        common_columns = list(set(source_cols) & set(target_cols))
        non_pk_columns = [col for col in common_columns if col not in primary_keys]

        if not non_pk_columns:
            update_clause = "NOTHING"
        else:
            update_set_clause = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in non_pk_columns)
            # Always reset the is_deleted flag on update, as the record is clearly active.
            if 'is_deleted' in target_cols:
                update_set_clause += ", is_deleted = FALSE"
            update_clause = f"UPDATE SET {update_set_clause}"

        conflict_target = ", ".join(f'"{pk}"' for pk in primary_keys)
        # The columns for INSERT must only be those present in the source table
        insert_columns = ", ".join(f'"{col}"' for col in source_cols)

        sql = f"""
        WITH results AS (
            INSERT INTO {target_table} ({insert_columns})
            SELECT {insert_columns} FROM {source_table}
            ON CONFLICT ({conflict_target}) DO {update_clause}
            RETURNING xmax = 0 AS is_insert
        )
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN is_insert THEN 1 ELSE 0 END) AS inserted_count
        FROM results;
        """

        result = self.execute_sql(sql, fetch='one')
        total_rows, inserted_count = (result[0] or 0), (result[1] or 0)

        updated_count = total_rows - inserted_count

        logger.info(f"Merge complete. Inserted: {inserted_count}, Updated: {updated_count}")
        return {"inserted": inserted_count, "updated": updated_count}

    def soft_delete_obsolete_records(self, source_table: str, target_table: str, primary_keys: List[str]) -> int:
        """
        Marks records in the target table as deleted if they do not exist in the source table.
        This is the "soft delete" operation.
        Returns:
            The number of records marked as obsolete.
        """
        logger.info(f"Marking obsolete records in '{target_table}'...")
        pk_join_clause = " AND ".join(f't."{pk}" = s."{pk}"' for pk in primary_keys)

        # We use a LEFT JOIN from target to source. If a source record is NULL,
        # it means the target record is obsolete.
        # We only update records that are not already marked as deleted.
        sql = f"""
        WITH obsolete_rows AS (
            SELECT t.ctid
            FROM {target_table} t
            LEFT JOIN {source_table} s ON {pk_join_clause}
            WHERE s."{primary_keys[0]}" IS NULL AND t.is_deleted = FALSE
        )
        UPDATE {target_table}
        SET is_deleted = TRUE
        WHERE ctid IN (SELECT ctid FROM obsolete_rows)
        RETURNING 1;
        """

        # We use RETURNING to count the number of updated rows
        results = self.execute_sql(sql, fetch='all')
        obsolete_count = len(results)

        if obsolete_count > 0:
            logger.info(f"Marked {obsolete_count} records as obsolete in '{target_table}'.")
        else:
            logger.info(f"No records needed to be marked as obsolete in '{target_table}'.")

        return obsolete_count

    def optimize_pre_load(self, schema: str) -> None:
        """Disables constraints and indexes before a full load."""
        # pg_restore handles this by default (data is loaded first, then indexes/constraints are created)
        pass

    def optimize_post_load(self, schema: str) -> None:
        """Re-enables constraints, rebuilds indexes, and runs statistics gathering (e.g., ANALYZE)."""
        logger.info("Optimizing database post-load: Running ANALYZE...")
        conn = self.connect()
        # Autocommit mode to run ANALYZE outside a transaction block
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with conn.cursor() as cursor:
                cursor.execute("ANALYZE VERBOSE;")
            logger.info("ANALYZE command completed successfully.")
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to run ANALYZE on the database: {e}") from e
        finally:
            # It's good practice to reset the isolation level
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_DEFAULT)


    def clean_schema(self, schema: str) -> None:
        """Drops and recreates a schema."""
        logger.info(f"Cleaning schema '{schema}'...")
        self.execute_sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        self.execute_sql(f"CREATE SCHEMA {schema};")
        logger.info(f"Schema '{schema}' has been reset.")

    def get_table_names(self, schema: str) -> List[str]:
        """Returns a list of table names in a given schema."""
        logger.debug(f"Fetching table names from schema '{schema}'...")
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name;
        """
        results = self.execute_sql(sql, (schema,), fetch='all')
        return [row[0] for row in results]

    def get_column_names(self, schema: str, table_name: str) -> List[str]:
        """Returns a list of column names for a given table in a schema."""
        logger.debug(f"Fetching column names for '{schema}.{table_name}'...")
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """
        results = self.execute_sql(sql, (schema, table_name), fetch='all')
        return [row[0] for row in results]

    def migrate_schema(self, source_schema: str, source_table_name: str, target_schema: str, target_table_name: str):
        """
        Compares the schema of a source and target table. If the target table does not exist,
        it is created. If it exists, additive changes (new columns) are applied.
        """
        logger.info(f"Comparing schema for table '{target_table_name}'...")

        # Check if target table exists
        table_exists_sql = "SELECT to_regclass(%s);"
        target_table_regclass = f'"{target_schema}"."{target_table_name}"'
        result = self.execute_sql(table_exists_sql, (target_table_regclass,), fetch='one')

        if result[0] is None:
            # Target table does not exist, so create it based on the source table's structure
            logger.info(f"Target table '{target_table_regclass}' does not exist. Creating it now.")
            create_sql = f'CREATE TABLE {target_table_regclass} (LIKE "{source_schema}"."{source_table_name}" INCLUDING ALL);'
            self.execute_sql(create_sql)
            # Also add the is_deleted column for soft-delete tracking
            add_col_sql = f'ALTER TABLE {target_table_regclass} ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;'
            self.execute_sql(add_col_sql)
            logger.info(f"Table '{target_table_regclass}' created and prepared for loading.")
            return  # No further migration needed

        # --- If table exists, proceed with column comparison ---
        source_cols_sql = "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_schema = %s AND table_name = %s;"
        source_cols_result = self.execute_sql(source_cols_sql, (source_schema, source_table_name), fetch='all')
        source_columns = {row[0]: {'type': row[1], 'length': row[2]} for row in source_cols_result}

        target_cols_sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s;"
        target_cols_result = self.execute_sql(target_cols_sql, (target_schema, target_table_name), fetch='all')
        target_columns = {row[0] for row in target_cols_result}

        # Ensure the is_deleted column exists for soft deletes
        if 'is_deleted' not in target_columns:
            logger.info(f"Adding 'is_deleted' column to target table '{target_table_name}' for soft-delete tracking.")
            add_col_sql = f'ALTER TABLE {target_table_regclass} ADD COLUMN is_deleted BOOLEAN NOT NULL DEFAULT FALSE;'
            self.execute_sql(add_col_sql)
            target_columns.add('is_deleted')

        new_columns = source_columns.keys() - target_columns

        if not new_columns:
            logger.info("Schemas are identical. No migration needed.")
            return

        logger.info(f"Found new columns to add: {', '.join(new_columns)}")

        for col_name in sorted(list(new_columns)):
            col_info = source_columns[col_name]
            data_type = col_info['type']
            if col_info['length']:
                data_type = f"{data_type}({col_info['length']})"

            alter_sql = f'ALTER TABLE {target_table_regclass} ADD COLUMN "{col_name}" {data_type};'
            logger.info(f"Applying schema change: {alter_sql}")
            self.execute_sql(alter_sql)

        logger.info(f"Successfully migrated schema for table '{target_table_name}'.")

    def _run_psql_restore(self, dump_archive_path: Path, schema: str):
        """
        Decompresses a .sql.gz dump and loads it into a specific schema using psql.
        This version redirects stdout/stderr to temp files to avoid I/O deadlocks.
        """
        if not shutil.which("psql"):
            raise FileNotFoundError("'psql' command not found. Please ensure the PostgreSQL client tools are installed and in your system's PATH.")

        logger.info(f"Initiating PSQL restore from '{dump_archive_path}' into schema '{schema}'...")
        env = os.environ.copy()
        env.update({
            "PGHOST": self._parsed_uri.hostname or "localhost",
            "PGPORT": str(self._parsed_uri.port or 5432),
            "PGUSER": self._parsed_uri.username or "",
        })
        if self._parsed_uri.password:
            env["PGPASSWORD"] = self._parsed_uri.password

        self.execute_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        command = ["psql", f"--dbname={self._parsed_uri.path.lstrip('/')}", "--quiet", "--no-psqlrc", "--single-transaction", "-v", "ON_ERROR_STOP=1"]

        try:
            with gzip.open(dump_archive_path, 'rt', encoding='utf-8') as f:
                sql_content = f.read()

            full_script = f'SET search_path = "{schema}", public;\n{sql_content}'

            with tempfile.TemporaryFile(mode="w+") as stdout_f, tempfile.TemporaryFile(mode="w+") as stderr_f:
                process = subprocess.run(command, input=full_script, text=True, env=env, stdout=stdout_f, stderr=stderr_f)

                if process.returncode != 0:
                    stdout_f.seek(0)
                    stderr_f.seek(0)
                    stdout = stdout_f.read()
                    stderr = stderr_f.read()
                    logger.error(f"psql failed with exit code {process.returncode}", extra={"stdout": stdout, "stderr": stderr})
                    raise RuntimeError(f"psql failed. See logs for details.")

            logger.info(f"Successfully loaded dump into schema '{schema}'.")
        except FileNotFoundError:
            raise RuntimeError("psql command not found. Make sure PostgreSQL client tools are installed and in your PATH.")
        except Exception as e:
            raise RuntimeError(f"An error occurred during psql execution: {e}") from e

    def _run_pg_restore(self, dump_archive_path: Path, schema: str | None = None, table_list: List[str] | None = None):
        """
        Extracts the dump and runs pg_restore, redirecting output to temp files
        to avoid I/O deadlocks from large amounts of output.
        """
        if not shutil.which("pg_restore"):
            raise FileNotFoundError("'pg_restore' command not found. Please ensure the PostgreSQL client tools are installed and in your system's PATH.")

        with tempfile.TemporaryDirectory() as tmpdir:
            logger.info(f"Extracting '{dump_archive_path.name}' to a temporary directory...")
            try:
                with tarfile.open(dump_archive_path, "r:gz") as tar:
                    tar.extractall(path=tmpdir)
            except tarfile.TarError as e:
                raise IOError(f"Failed to extract tar.gz file: {e}") from e

            extracted_dirs = [d for d in Path(tmpdir).iterdir() if d.is_dir()]
            if not extracted_dirs:
                raise FileNotFoundError("No directory found in the extracted ChEMBL archive.")
            dump_dir = extracted_dirs[0]

            jobs = os.cpu_count() or 4
            command = ["pg_restore", "--exit-on-error", f"--dbname={self._parsed_uri.path.lstrip('/')}", f"--jobs={jobs}", "--no-owner", "--no-privileges"]
            if schema:
                self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                command.append(f"--schema={schema}")

            if table_list:
                logger.info(f"Will restore only the following tables: {', '.join(table_list)}")
                for table in table_list:
                    command.extend(["--table", table])
            else:
                logger.info("Will restore all tables from dump.")

            command.append(str(dump_dir))

            env = os.environ.copy()
            env.update({
                "PGHOST": self._parsed_uri.hostname or "localhost",
                "PGPORT": str(self._parsed_uri.port or 5432),
                "PGUSER": self._parsed_uri.username or "",
            })
            if self._parsed_uri.password:
                env["PGPASSWORD"] = self._parsed_uri.password

            logger.info(f"Running pg_restore with {jobs} parallel jobs...")
            try:
                with tempfile.TemporaryFile(mode="w+") as stdout_f, tempfile.TemporaryFile(mode="w+") as stderr_f:
                    process = subprocess.run(command, text=True, env=env, stdout=stdout_f, stderr=stderr_f)

                    if process.returncode != 0:
                        stdout_f.seek(0)
                        stderr_f.seek(0)
                        stdout = stdout_f.read()
                        stderr = stderr_f.read()
                        logger.error(f"pg_restore failed with exit code {process.returncode}", extra={"stdout": stdout, "stderr": stderr})
                        raise RuntimeError(f"pg_restore failed. See logs for details.")
            except FileNotFoundError:
                raise RuntimeError("pg_restore command not found. Make sure PostgreSQL client tools are installed and in your PATH.")
            except Exception as e:
                 raise RuntimeError(f"An error occurred during pg_restore execution: {e}") from e
