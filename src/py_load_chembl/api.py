from pathlib import Path
from typing import List, Optional
import logging

from py_load_chembl.adapters.factory import get_adapter
from py_load_chembl.pipeline import LoaderPipeline
from py_load_chembl.logging_config import setup_logging

logger = logging.getLogger(__name__)


def full_load(
    connection_string: str,
    chembl_version: str = "latest",
    output_dir: Path = Path("./chembl_data"),
    include_tables: Optional[List[str]] = None,
):
    """
    Performs a full load of a specific ChEMBL version into a target database.
    This will completely wipe the target schema before loading.

    Args:
        connection_string: The database connection string (e.g., "postgresql://user:pass@host/db").
        chembl_version: The ChEMBL version to load (e.g., "33" or "latest"). Defaults to "latest".
        output_dir: The directory to store downloaded ChEMBL files. Defaults to "./chembl_data".
        include_tables: A list of specific tables to load. If None, all tables are loaded.
    """
    setup_logging()
    logger.info("Starting ChEMBL full load via Python API.")

    adapter = get_adapter(connection_string)
    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="FULL",
        output_dir=output_dir,
        include_tables=include_tables,
    )
    pipeline.run()
    logger.info("ChEMBL full load completed successfully.")


def delta_load(
    connection_string: str,
    chembl_version: str,
    output_dir: Path = Path("./chembl_data"),
    include_tables: Optional[List[str]] = None,
):
    """
    Performs a delta load to update an existing ChEMBL database with a newer release.

    Args:
        connection_string: The database connection string (e.g., "postgresql://user:pass@host/db").
        chembl_version: The ChEMBL version to load (e.g., "34"). Cannot be "latest" for a delta load.
        output_dir: The directory to store downloaded ChEMBL files. Defaults to "./chembl_data".
        include_tables: A list of specific tables to update. If None, all tables are updated.
    """
    setup_logging()
    logger.info("Starting ChEMBL delta load via Python API.")

    if chembl_version.lower() == "latest":
        raise ValueError("A specific ChEMBL version must be provided for a delta load.")

    adapter = get_adapter(connection_string)
    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="DELTA",
        output_dir=output_dir,
        include_tables=include_tables,
    )
    pipeline.run()
    logger.info("ChEMBL delta load completed successfully.")
