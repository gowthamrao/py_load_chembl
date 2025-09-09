import datetime
import logging
from pathlib import Path
from typing import List
from py_load_chembl._version import __version__
from py_load_chembl.adapters.base import DatabaseAdapter
from py_load_chembl import downloader
from py_load_chembl.schema_parser import parse_chembl_ddl

logger = logging.getLogger(__name__)


class LoaderPipeline:
    """
    The main pipeline for loading ChEMBL data.
    """

    def __init__(
        self,
        version: str,
        output_dir: Path,
        adapter: DatabaseAdapter | None = None,
        mode: str | None = None,
        include_tables: List[str] | None = None,
    ):
        if mode is None and adapter is None:
            # This is valid for download-only mode
            pass
        elif mode is None or adapter is None:
            raise ValueError(
                "Both 'mode' and 'adapter' must be provided for loading operations."
            )

        self.adapter = adapter
        self.version_str = version
        self.mode = mode.upper() if mode else None
        self.output_dir = output_dir
        self.include_tables = include_tables
        self.chembl_version = 0
        self.pg_dump_path: Path | None = None
        self.load_id: int | None = None

    def run(self):
        """
        Executes the loading pipeline.
        """
        logger.info(
            f"Starting ChEMBL load for version '{self.version_str}' in {self.mode} mode."
        )
        error = None
        try:
            self._log_start_to_db()

            # 1. Acquisition Stage
            self._acquire_data()

            if self.pg_dump_path:
                logger.info(
                    f"Successfully downloaded and verified ChEMBL {self.chembl_version} to {self.pg_dump_path}"
                )
            else:
                raise RuntimeError(
                    "Data acquisition failed to return a valid file path."
                )

            # 2. Loading Stage (Full or Delta)
            if self.mode == "FULL":
                self._execute_full_load()
            elif self.mode == "DELTA":
                self._execute_delta_load()
            else:
                raise ValueError(
                    f"Invalid mode: {self.mode}. Must be 'FULL' or 'DELTA'."
                )

        except Exception as e:
            error = e
            # Re-raise the exception to be handled by the CLI
            raise
        finally:
            # Always ensure the database log is finalized
            self._log_end_to_db(error)

    def _acquire_data(self):
        """Handles the data acquisition stage of the pipeline."""
        logger.info("--- Stage: Acquisition ---")
        if self.version_str.lower() == "latest":
            logger.info("Detecting latest ChEMBL version...")
            self.chembl_version = downloader.get_latest_chembl_version()
            logger.info(f"Latest version is: {self.chembl_version}")
        else:
            self.chembl_version = int(self.version_str)

        # For DELTA mode, we need the plain SQL dump to load into a staging schema.
        # For FULL mode, the custom .tar.gz format is faster with pg_restore.
        use_plain_sql = self.mode == "DELTA"
        dump_url, checksums_url = downloader.get_chembl_file_urls(
            self.chembl_version, plain_sql=use_plain_sql
        )

        logger.info(f"Requesting ChEMBL dump from: {dump_url}")
        # Check if file already exists and is valid before downloading
        local_file = self.output_dir / dump_url.split("/")[-1]
        if local_file.exists():
            logger.info(
                f"File '{local_file.name}' already exists. Verifying checksum..."
            )
            if downloader.verify_checksum(local_file, checksums_url):
                logger.info("Checksum is valid. Skipping download.")
                self.pg_dump_path = local_file
                return
            else:
                logger.warning("Checksum is invalid. Re-downloading the file.")

        downloaded_file = downloader.download_file(dump_url, self.output_dir)

        logger.info("Verifying file integrity...")
        is_valid = downloader.verify_checksum(downloaded_file, checksums_url)

        if not is_valid:
            raise ValueError(
                f"Checksum for {downloaded_file.name} is invalid. "
                "The file may be corrupted. Please delete it and try again."
            )

        self.pg_dump_path = downloaded_file
        logger.info("Checksum verified successfully.")

    def _execute_full_load(self):
        """Handles the full data load stage."""
        if not self.adapter or not self.pg_dump_path:
            raise RuntimeError("An adapter must be configured to execute a full load.")

        logger.info("--- Stage: Full Load ---")

        # In a full load, we first capture and drop all existing constraints and indexes from the target schema.
        # This is to ensure that the load is not slowed down by existing objects, and that any
        # objects not in the new dump are correctly removed.
        self.adapter.optimize_pre_load(schema="public")

        # Then, clean the public schema to ensure idempotency. This drops all existing tables.
        self.adapter.clean_schema("public")

        # The adapter's bulk_load_table for postgres uses pg_restore, which is a full restore.
        self.adapter.bulk_load_table(
            table_name="all",
            data_source=self.pg_dump_path,
            schema="public",
            options={"include_tables": self.include_tables},
        )
        log_table_name = (
            f"tables: {','.join(self.include_tables)}"
            if self.include_tables
            else "all_tables"
        )
        self._log_load_details_to_db(table_name=log_table_name, insert_count=-1)

        # 3. Post-Processing Stage
        logger.info("--- Stage: Post-Processing ---")
        self.adapter.optimize_post_load(schema="public")  # Schema is illustrative

    def _execute_delta_load(self):
        """
        Handles the delta data load stage by loading the new ChEMBL release into a
        temporary staging schema and then merging the data into the production schema.
        """
        if not self.adapter or not self.pg_dump_path:
            raise RuntimeError(
                "An adapter and data path must be configured for a delta load."
            )

        logger.info("--- Stage: Delta Load ---")
        staging_schema = f"staging_chembl_{self.chembl_version}"
        target_schema = "public"

        try:
            # 1. Load new data into a temporary staging schema
            logger.info(
                f"Loading ChEMBL v{self.chembl_version} into temporary schema '{staging_schema}'..."
            )
            # Note: The .sql.gz dump is required for delta mode.
            self.adapter.bulk_load_table(
                table_name="all",
                data_source=self.pg_dump_path,
                schema=staging_schema,
            )
            logger.info("Staging load complete.")

            # 2. Dynamically parse the DDL to get primary key information.
            chembl_schemas = parse_chembl_ddl(self.pg_dump_path)

            # 3. Get the list of tables to merge from the staging schema
            all_tables_in_staging = self.adapter.get_table_names(schema=staging_schema)
            if self.include_tables:
                tables_to_merge = [
                    t for t in all_tables_in_staging if t in self.include_tables
                ]
                logger.info(
                    f"Found {len(all_tables_in_staging)} total tables in staging, but will only process {len(tables_to_merge)} based on the include list."
                )
            else:
                tables_to_merge = all_tables_in_staging
                logger.info(
                    f"Found {len(tables_to_merge)} tables in staging schema to process."
                )

            # 4. Iterate and merge each table
            for table_name in tables_to_merge:
                logger.info(f"--- Processing table: {table_name} ---")

                # 4a. Compare schemas and migrate if necessary before merging
                self._perform_schema_migration(
                    source_schema=staging_schema,
                    target_schema=target_schema,
                    table_name=table_name,
                )

                # 4b. Get primary key info from our dynamically parsed schema
                table_schema_info = chembl_schemas.get(table_name)
                if not table_schema_info or not table_schema_info.primary_keys:
                    logger.warning(
                        f"No primary key definition found for table '{table_name}'. Skipping merge."
                    )
                    continue

                # 4c. Introspect the staging table to get all column names for the merge
                all_columns = self.adapter.get_column_names(
                    schema=staging_schema, table_name=table_name
                )
                if not all_columns:
                    logger.warning(
                        f"Could not find columns for table '{table_name}'. Skipping merge."
                    )
                    continue

                # 4d. Perform the merge from staging to production
                merge_stats = self.adapter.execute_merge(
                    source_table=f'{staging_schema}."{table_name}"',
                    target_table=f'{target_schema}."{table_name}"',
                    primary_keys=table_schema_info.primary_keys,
                    all_columns=all_columns,
                )
                self._log_load_details_to_db(
                    table_name=table_name,
                    insert_count=merge_stats.get("inserted", 0),
                    update_count=merge_stats.get("updated", 0),
                    obsolete_count=0,  # Obsolete count is handled in a separate step now
                )

            # 5. Handle obsolete records using the correct FRD-compliant method.
            # This is done once after all tables are merged.
            logger.info("--- Handling Obsolete Records ---")
            obsolete_count = self.adapter.handle_obsolete_records(
                source_schema=staging_schema,
                target_schema=target_schema,
            )
            self._log_load_details_to_db(
                table_name="chembl_id_lookup",
                insert_count=0,  # Already logged during its own merge
                update_count=0,  # Already logged
                obsolete_count=obsolete_count,
            )

        finally:
            # 6. Clean up by dropping the staging schema
            logger.info(f"Cleaning up temporary staging schema '{staging_schema}'...")
            self.adapter.execute_sql(f"DROP SCHEMA IF EXISTS {staging_schema} CASCADE;")
            logger.info("Cleanup complete.")

    def _perform_schema_migration(
        self, source_schema: str, target_schema: str, table_name: str
    ):
        """
        Orchestrates schema migration for a single table. It uses the adapter's
        generic methods to compare schemas and apply additive changes.
        This logic resides in the pipeline, not the adapter, to keep the adapter
        interface clean and focused on database-specific execution.
        """
        if not self.adapter:
            return

        logger.info(f"Checking for schema changes for table '{table_name}'...")

        # Check if target table exists
        target_table_exists = self.adapter.get_table_names(target_schema)
        if table_name not in target_table_exists:
            logger.info(
                f"Target table '{target_schema}.{table_name}' does not exist. Creating it."
            )
            # This LIKE statement is specific to PostgreSQL but is a reasonable default.
            # A more advanced implementation might use an abstract method in the adapter.
            create_sql = f'CREATE TABLE "{target_schema}"."{table_name}" (LIKE "{source_schema}"."{table_name}" INCLUDING ALL);'
            self.adapter.execute_sql(create_sql)
            logger.info(f"Table '{target_schema}.{table_name}' created.")
            return

        # Get column definitions from source and target to find new columns
        source_cols = self.adapter.get_column_definitions(
            schema=source_schema, table_name=table_name
        )
        target_cols = self.adapter.get_column_definitions(
            schema=target_schema, table_name=table_name
        )
        source_col_names = {c["name"] for c in source_cols}
        target_col_names = {c["name"] for c in target_cols}

        new_columns = source_col_names - target_col_names

        if not new_columns:
            logger.info(f"No schema changes found for table '{table_name}'.")
            return

        logger.info(
            f"Found new columns to add to '{table_name}': {', '.join(new_columns)}"
        )
        for col in source_cols:
            if col["name"] in new_columns:
                # Construct the full data type, e.g., "VARCHAR(255)"
                data_type = col["type"]
                if col.get("length"):
                    data_type = f"{data_type}({col['length']})"

                alter_sql = f'ALTER TABLE "{target_schema}"."{table_name}" ADD COLUMN "{col["name"]}" {data_type};'
                logger.info(f"Applying schema change: {alter_sql}")
                self.adapter.execute_sql(alter_sql)

        logger.info(f"Successfully migrated schema for table '{table_name}'.")

    def _log_start_to_db(self):
        """Initializes metadata tables and logs the start of the process to the database."""
        if not self.adapter:
            return
        self.adapter.create_metadata_tables()
        sql = """
            INSERT INTO chembl_loader_meta.load_history
            (chembl_version, load_type, start_timestamp, status, loader_version)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING load_id;
        """
        params = (
            str(self.chembl_version) if self.chembl_version else self.version_str,
            self.mode,
            datetime.datetime.now(datetime.timezone.utc),
            "RUNNING",
            __version__,
        )
        result = self.adapter.execute_sql(sql, params, fetch="one")
        self.load_id = result[0]
        logger.info(f"Logging to database with load_id: {self.load_id}")

    def _log_end_to_db(self, error: Exception | None):
        """Updates the metadata log in the database with the final status of the load."""
        if not self.adapter or not self.load_id:
            return

        status = "FAILED" if error else "SUCCESS"
        error_message = str(error) if error else None

        logger.info(
            f"Finalizing load_id {self.load_id} in database with status: {status}"
        )

        sql = """
            UPDATE chembl_loader_meta.load_history
            SET end_timestamp = %s, status = %s, error_message = %s
            WHERE load_id = %s;
        """
        params = (
            datetime.datetime.now(datetime.timezone.utc),
            status,
            error_message,
            self.load_id,
        )
        self.adapter.execute_sql(sql, params)

    def _log_load_details_to_db(
        self,
        table_name: str,
        stage_record_count: int = 0,
        insert_count: int = 0,
        update_count: int = 0,
        obsolete_count: int = 0,
    ):
        """Logs the details for a specific table load to the database."""
        if not self.adapter or not self.load_id:
            return

        sql = """
            INSERT INTO chembl_loader_meta.load_details
            (load_id, table_name, stage_record_count, insert_count, update_count, obsolete_count)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        params = (
            self.load_id,
            table_name,
            stage_record_count,
            insert_count,
            update_count,
            obsolete_count,
        )
        self.adapter.execute_sql(sql, params)
