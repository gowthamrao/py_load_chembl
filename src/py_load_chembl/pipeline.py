from pathlib import Path
from py_load_chembl.adapters.base import DatabaseAdapter
from py_load_chembl import downloader


class LoaderPipeline:
    """
    The main pipeline for loading ChEMBL data.
    """

    def __init__(self, adapter: DatabaseAdapter, version: str, mode: str, output_dir: Path):
        self.adapter = adapter
        self.version_str = version
        self.mode = mode
        self.output_dir = output_dir
        self.chembl_version = 0
        self.pg_dump_path: Path | None = None

    def run(self):
        """
        Executes the loading pipeline.
        """
        print(f"Starting ChEMBL load for version '{self.version_str}' in {self.mode.upper()} mode.")

        # 1. Acquisition Stage
        self._acquire_data()

        if self.pg_dump_path:
            print(f"Successfully downloaded and verified ChEMBL {self.chembl_version} to {self.pg_dump_path}")

        # Subsequent stages (Staging, Merge, etc.) will be added here.

    def _acquire_data(self):
        """Handles the data acquisition stage of the pipeline."""
        print("--- Stage: Acquisition ---")
        if self.version_str.lower() == "latest":
            print("Detecting latest ChEMBL version...")
            self.chembl_version = downloader.get_latest_chembl_version()
            print(f"Latest version is: {self.chembl_version}")
        else:
            self.chembl_version = int(self.version_str)

        pg_dump_url, checksums_url = downloader.get_chembl_file_urls(self.chembl_version)

        print(f"Downloading PostgreSQL dump from: {pg_dump_url}")
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
