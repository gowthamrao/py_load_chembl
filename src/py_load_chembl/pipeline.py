import datetime
from pathlib import Path
from py_load_chembl import __version__
from py_load_chembl.adapters.base import DatabaseAdapter
from py_load_chembl import downloader
from py_load_chembl.db_schemas import CHEMBL_SCHEMAS, CHEMBL_TABLE_DEPENDENCIES
from py_load_chembl.utils import topological_sort_tables


class LoaderPipeline:
    """
    The main pipeline for loading ChEMBL data.
    """

    def __init__(self, version: str, output_dir: Path, adapter: DatabaseAdapter | None = None, mode: str | None = None):
        if mode is None and adapter is None:
            # This is valid for download-only mode
            pass
        elif mode is None or adapter is None:
            raise ValueError("Both 'mode' and 'adapter' must be provided for loading operations.")

        self.adapter = adapter
        self.version_str = version
        self.mode = mode.upper() if mode else None
        self.output_dir = output_dir
        self.chembl_version = 0
        self.pg_dump_path: Path | None = None
        self.load_id: int | None = None

    def run(self):
        """
        Executes the loading pipeline.
        """
        print(f"Starting ChEMBL load for version '{self.version_str}' in {self.mode} mode.")
        error = None
        try:
            self._setup_logging()

            # 1. Acquisition Stage
            self._acquire_data()

            if self.pg_dump_path:
                print(f"Successfully downloaded and verified ChEMBL {self.chembl_version} to {self.pg_dump_path}")
            else:
                raise RuntimeError("Data acquisition failed to return a valid file path.")

            # 2. Loading Stage (Full or Delta)
            if self.mode == 'FULL':
                self._execute_full_load()
            elif self.mode == 'DELTA':
                self._execute_delta_load()
            else:
                raise ValueError(f"Invalid mode: {self.mode}. Must be 'FULL' or 'DELTA'.")

        except Exception as e:
            error = e
            # Re-raise the exception after logging
            raise
        finally:
            self._finalize_logging(error)

    def _acquire_data(self):
        """Handles the data acquisition stage of the pipeline."""
        print("\n--- Stage: Acquisition ---")
        if self.version_str.lower() == "latest":
            print("Detecting latest ChEMBL version...")
            self.chembl_version = downloader.get_latest_chembl_version()
            print(f"Latest version is: {self.chembl_version}")
        else:
            self.chembl_version = int(self.version_str)

        # For DELTA mode, we need the plain SQL dump to load into a staging schema.
        # For FULL mode, the custom .tar.gz format is faster with pg_restore.
        use_plain_sql = self.mode == 'DELTA'
        dump_url, checksums_url = downloader.get_chembl_file_urls(self.chembl_version, plain_sql=use_plain_sql)

        print(f"Requesting ChEMBL dump from: {dump_url}")
        # Check if file already exists and is valid before downloading
        local_file = self.output_dir / dump_url.split("/")[-1]
        if local_file.exists():
            print(f"File '{local_file.name}' already exists. Verifying checksum...")
            if downloader.verify_checksum(local_file, checksums_url):
                print("Checksum is valid. Skipping download.")
                self.pg_dump_path = local_file
                return
            else:
                print("Checksum is invalid. Re-downloading the file.")

        downloaded_file = downloader.download_file(dump_url, self.output_dir)

        print("Verifying file integrity...")
        is_valid = downloader.verify_checksum(downloaded_file, checksums_url)

        if not is_valid:
            raise ValueError(
                f"Checksum for {downloaded_file.name} is invalid. "
                "The file may be corrupted. Please delete it and try again."
            )

        self.pg_dump_path = downloaded_file
        print("Checksum verified successfully.")

    def _execute_full_load(self):
        """Handles the full data load stage."""
        if not self.adapter:
            raise RuntimeError("An adapter must be configured to execute a full load.")

        print("\n--- Stage: Full Load ---")

        # Clean the public schema to ensure idempotency. This drops all existing tables.
        self.adapter.clean_schema("public")

        # The adapter's bulk_load_table for postgres uses pg_restore, which is a full restore.
        # The table_name parameter is ignored in this specific implementation.
        self.adapter.bulk_load_table(
            table_name="all",  # Parameter is ignored by pg_restore implementation
            data_source=self.pg_dump_path,
            schema="public",  # Explicitly restore to public schema
        )
        self._log_load_details(table_name="all_tables_via_pg_restore", insert_count=-1)

        # 3. Post-Processing Stage
        print("\n--- Stage: Post-Processing ---")
        self.adapter.optimize_post_load(schema="public") # Schema is illustrative

    def _execute_delta_load(self):
        """
        Handles the delta data load stage by loading the new ChEMBL release into a
        temporary staging schema and then merging the data into the production schema.
        """
        if not self.adapter or not self.pg_dump_path:
            raise RuntimeError("An adapter and data path must be configured for a delta load.")

        print("\n--- Stage: Delta Load ---")
        staging_schema = f"staging_chembl_{self.chembl_version}"
        target_schema = "public"

        try:
            # 1. Load new data into a temporary staging schema
            print(f"Loading ChEMBL v{self.chembl_version} into temporary schema '{staging_schema}'...")
            self.adapter.bulk_load_table(
                table_name="all",  # This is ignored by the pg_restore implementation
                data_source=self.pg_dump_path,
                schema=staging_schema,
            )
            print("Staging load complete.")

            # 2. Get the list of tables to merge from the staging schema
            staged_tables = self.adapter.get_table_names(schema=staging_schema)
            print(f"Found {len(staged_tables)} tables in staging schema to process.")

            # Sort the tables topologically to respect foreign key constraints
            print("Sorting tables by dependency order...")
            tables_to_merge = topological_sort_tables(staged_tables, CHEMBL_TABLE_DEPENDENCIES)
            print(f"Merge order: {', '.join(tables_to_merge)}")

            # 3. Iterate and merge each table
            for table_name in tables_to_merge:
                print(f"\n--- Processing table: {table_name} ---")

                # 3a. Compare schemas and migrate if necessary before merging
                self.adapter.migrate_schema(
                    source_schema=staging_schema,
                    source_table_name=table_name,
                    target_schema=target_schema,
                    target_table_name=table_name,
                )

                table_schema_info = CHEMBL_SCHEMAS.get(table_name)
                if not table_schema_info or not table_schema_info.primary_keys:
                    print(f"Warning: No primary key definition for table '{table_name}'. Skipping merge.")
                    continue

                # Introspect the staging table to get all column names for the merge
                all_columns = self.adapter.get_column_names(schema=staging_schema, table_name=table_name)
                if not all_columns:
                    print(f"Warning: Could not find columns for table '{table_name}'. Skipping merge.")
                    continue

                # Perform the merge from staging to production
                merge_stats = self.adapter.execute_merge(
                    source_table=f'{staging_schema}."{table_name}"',
                    target_table=f'{target_schema}."{table_name}"',
                    primary_keys=table_schema_info.primary_keys,
                    all_columns=all_columns,
                )

                self._log_load_details(
                    table_name=table_name,
                    insert_count=merge_stats.get("inserted", 0),
                    update_count=merge_stats.get("updated", 0),
                )

        finally:
            # 4. Clean up by dropping the staging schema
            print(f"\nCleaning up temporary staging schema '{staging_schema}'...")
            self.adapter.execute_sql(f"DROP SCHEMA IF EXISTS {staging_schema} CASCADE;")
            print("Cleanup complete.")

    def _setup_logging(self):
        """Initializes metadata tables and logs the start of the process."""
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
            'RUNNING',
            __version__
        )
        result = self.adapter.execute_sql(sql, params, fetch='one')
        self.load_id = result[0]
        print(f"Logging to load_id: {self.load_id}")

    def _finalize_logging(self, error: Exception | None):
        """Updates the metadata log with the final status of the load."""
        if not self.adapter or not self.load_id:
            return

        status = 'FAILED' if error else 'SUCCESS'
        error_message = str(error) if error else None

        print(f"Finalizing load_id {self.load_id} with status: {status}")

        sql = """
            UPDATE chembl_loader_meta.load_history
            SET end_timestamp = %s, status = %s, error_message = %s
            WHERE load_id = %s;
        """
        params = (
            datetime.datetime.now(datetime.timezone.utc),
            status,
            error_message,
            self.load_id
        )
        self.adapter.execute_sql(sql, params)

    def _log_load_details(self, table_name: str, stage_record_count: int = 0, insert_count: int = 0, update_count: int = 0, obsolete_count: int = 0):
        """Logs the details for a specific table load."""
        if not self.adapter or not self.load_id:
            return

        sql = """
            INSERT INTO chembl_loader_meta.load_details
            (load_id, table_name, stage_record_count, insert_count, update_count, obsolete_count)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        params = (self.load_id, table_name, stage_record_count, insert_count, update_count, obsolete_count)
        self.adapter.execute_sql(sql, params)
