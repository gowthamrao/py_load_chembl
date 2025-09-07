from pathlib import Path
from py_load_chembl.adapters.base import DatabaseAdapter
from py_load_chembl import downloader


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

    def run(self):
        """
        Executes the loading pipeline.
        """
        print(f"Starting ChEMBL load for version '{self.version_str}' in {self.mode} mode.")

        # 1. Acquisition Stage
        self._acquire_data()

        if self.pg_dump_path:
            print(f"Successfully downloaded and verified ChEMBL {self.chembl_version} to {self.pg_dump_path}")
        else:
            # This should not happen if _acquire_data works correctly
            raise RuntimeError("Data acquisition failed to return a valid file path.")

        # 2. Loading Stage (Full or Delta)
        if self.mode == 'FULL':
            self._execute_full_load()
        elif self.mode == 'DELTA':
            print("--- Stage: Delta Load ---")
            print("Delta loading is not yet implemented.")
            # Placeholder for delta load logic
        else:
            raise ValueError(f"Invalid mode: {self.mode}. Must be 'FULL' or 'DELTA'.")

    def _acquire_data(self):
        """Handles the data acquisition stage of the pipeline."""
        print("\n--- Stage: Acquisition ---")
        if self.version_str.lower() == "latest":
            print("Detecting latest ChEMBL version...")
            self.chembl_version = downloader.get_latest_chembl_version()
            print(f"Latest version is: {self.chembl_version}")
        else:
            self.chembl_version = int(self.version_str)

        # For now, we only support postgres, so we directly get the pg_dump url
        pg_dump_url, checksums_url = downloader.get_chembl_file_urls(self.chembl_version)

        print(f"Downloading PostgreSQL dump from: {pg_dump_url}")
        # Check if file already exists and is valid before downloading
        local_file = self.output_dir / pg_dump_url.split("/")[-1]
        if local_file.exists():
            print(f"File '{local_file.name}' already exists. Verifying checksum...")
            if downloader.verify_checksum(local_file, checksums_url):
                print("Checksum is valid. Skipping download.")
                self.pg_dump_path = local_file
                return
            else:
                print("Checksum is invalid. Re-downloading the file.")

        downloaded_file = downloader.download_file(pg_dump_url, self.output_dir)

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

        # The adapter's bulk_load_table for postgres uses pg_restore, which is a full restore.
        # The table_name parameter is ignored in this specific implementation.
        self.adapter.bulk_load_table(
            table_name="all", # Parameter is ignored by pg_restore implementation
            data_source=self.pg_dump_path
        )

        # 3. Post-Processing Stage
        print("\n--- Stage: Post-Processing ---")
        self.adapter.optimize_post_load(schema="public") # Schema is illustrative
