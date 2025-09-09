import hashlib
import logging
import re
from pathlib import Path
from typing import Tuple

import requests
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

logger = logging.getLogger(__name__)
BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases"


class ChecksumError(ValueError):
    """Custom exception for checksum validation errors."""


def download_chembl_db(
    version: int, output_dir: Path, plain_sql: bool = False
) -> Path:
    """
    Downloads the ChEMBL database dump for a specific version and verifies its integrity.

    This is the main public function for this module. It orchestrates the download
    and checksum verification process.

    Args:
        version: The ChEMBL version number to download.
        output_dir: The directory where the file will be saved.
        plain_sql: If True, downloads the plain-text SQL dump. Otherwise, downloads
                   the pg_restore custom format archive.

    Returns:
        The path to the downloaded and verified file.

    Raises:
        ChecksumError: If the downloaded file's checksum does not match the official one.
    """
    logger.info(
        f"Starting download and verification for ChEMBL v{version} (plain_sql={plain_sql})."
    )
    dump_url, checksums_url = get_chembl_file_urls(version, plain_sql=plain_sql)

    downloaded_file_path = download_file(dump_url, output_dir)

    try:
        is_valid = verify_checksum(downloaded_file_path, checksums_url)
        if not is_valid:
            logger.error(
                f"Checksum validation failed for {downloaded_file_path.name}. Deleting corrupted file."
            )
            downloaded_file_path.unlink()  # Delete the corrupted file
            raise ChecksumError(
                f"Checksum for {downloaded_file_path.name} is invalid. The file has been deleted."
            )
    except Exception:
        # If anything goes wrong during verification, clean up the downloaded file
        if downloaded_file_path.exists():
            downloaded_file_path.unlink()
        raise

    logger.info(f"Successfully downloaded and verified {downloaded_file_path.name}.")
    return downloaded_file_path


def get_latest_chembl_version() -> int:
    """
    Finds the latest ChEMBL version number from the EBI FTP server.
    """
    logger.info(f"Querying EBI FTP server for latest ChEMBL version at: {BASE_URL}")
    try:
        response = requests.get(BASE_URL + "/", timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        raise ConnectionError(f"Could not connect to ChEMBL FTP server: {e}") from e

    dir_names = re.findall(r'href="chembl_(\d+)/"', response.text)
    if not dir_names:
        raise ValueError("Could not find any ChEMBL versions in the FTP directory.")

    latest_version = max(int(v) for v in dir_names)
    logger.info(f"Detected latest ChEMBL version: {latest_version}")
    return latest_version


def get_chembl_file_urls(version: int, plain_sql: bool = False) -> Tuple[str, str]:
    """
    Constructs the URLs for the PostgreSQL dump and checksums file for a given version.
    """
    version_url = f"{BASE_URL}/chembl_{version}"
    if plain_sql:
        dump_url = f"{version_url}/chembl_{version}_postgresql.sql.gz"
    else:
        dump_url = f"{version_url}/chembl_{version}_postgresql.tar.gz"
    checksums_url = f"{version_url}/checksums.txt"
    return dump_url, checksums_url


def download_file(url: str, output_dir: Path, resume: bool = True) -> Path:
    """
    Downloads a file from a URL to a local directory, with progress bar and resume capability.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    local_filename = url.split("/")[-1]
    local_path = output_dir / local_filename

    headers = {}
    file_mode = "wb"
    initial_size = 0

    if resume and local_path.exists():
        initial_size = local_path.stat().st_size
        headers["Range"] = f"bytes={initial_size}-"
        file_mode = "ab"
        logger.info(
            f"Resuming download for {local_filename} from {initial_size} bytes."
        )
    else:
        logger.info(f"Starting new download for {local_filename}.")

    try:
        with requests.get(url, stream=True, headers=headers, timeout=60) as r:
            # Check if the server supports range requests for resuming
            if r.status_code == 206:
                logger.info("Server supports resume.")
            elif initial_size > 0:
                logger.warning(
                    "Server does not support resume. Restarting download from scratch."
                )
                initial_size = 0
                file_mode = "wb"

            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0)) + initial_size

            with Progress(
                TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
                BarColumn(bar_width=None),
                "[progress.percentage]{task.percentage:>3.1f}%",
                "•",
                DownloadColumn(),
                "•",
                TransferSpeedColumn(),
                "•",
                TimeRemainingColumn(),
            ) as progress:
                task_id = progress.add_task(
                    "download",
                    total=total_size,
                    completed=initial_size,
                    filename=local_filename,
                )
                with open(local_path, file_mode) as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        progress.update(task_id, advance=len(chunk))
    except requests.RequestException as e:
        raise ConnectionError(f"Failed to download {url}: {e}") from e

    logger.info(f"Finished downloading {local_filename}.")
    return local_path


def verify_checksum(file_path: Path, checksums_url: str) -> bool:
    """
    Verifies the MD5 checksum of a downloaded file.
    """
    logger.info(
        f"Verifying checksum for {file_path.name} using checksums from {checksums_url}"
    )
    try:
        response = requests.get(checksums_url, timeout=30)
        response.raise_for_status()
        checksums_text = response.text
    except requests.RequestException as e:
        raise ConnectionError(
            f"Could not download checksums file from {checksums_url}: {e}"
        ) from e

    expected_checksum = None
    for line in checksums_text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == file_path.name:
            expected_checksum = parts[0]
            break

    if not expected_checksum:
        raise ValueError(
            f"Could not find checksum for {file_path.name} in {checksums_url}"
        )

    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    actual_checksum = hasher.hexdigest()

    is_valid = actual_checksum == expected_checksum
    if is_valid:
        logger.info(
            "Checksum valid for %s.",
            file_path.name,
            extra={"expected": expected_checksum, "actual": actual_checksum},
        )
    else:
        logger.warning(
            "Checksum invalid for %s.",
            file_path.name,
            extra={"expected": expected_checksum, "actual": actual_checksum},
        )

    return is_valid
