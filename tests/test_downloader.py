import hashlib
from pathlib import Path
import pytest
from py_load_chembl import downloader

MOCK_FTP_INDEX_HTML = """
<a href="chembl_33/">chembl_33/</a>
<a href="chembl_34/">chembl_34/</a>
<a href="chembl_32/">chembl_32/</a>
"""

MOCK_CHECKSUMS_TXT = """
a1b2c3d4e5f6  chembl_34_postgresql.tar.gz
g7h8i9j0k1l2  chembl_34_sqlite.tar.gz
"""

def test_get_latest_chembl_version(requests_mock):
    """Test that the latest version is correctly parsed from the FTP index."""
    requests_mock.get(downloader.BASE_URL + "/", text=MOCK_FTP_INDEX_HTML)
    latest_version = downloader.get_latest_chembl_version()
    assert latest_version == 34

def test_get_chembl_file_urls():
    """Test that the file URLs are constructed correctly for both dump formats."""
    # Test the new default behavior (plain SQL dump)
    sql_url, checksums_url_1 = downloader.get_chembl_file_urls(34)
    assert sql_url == "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_34/chembl_34_postgresql.sql.gz"
    assert checksums_url_1 == "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_34/checksums.txt"

    # Test the old behavior (pg_restore dump)
    tar_url, checksums_url_2 = downloader.get_chembl_file_urls(34, plain_sql=False)
    assert tar_url == "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_34/chembl_34_postgresql.tar.gz"
    assert checksums_url_2 == "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_34/checksums.txt"

def test_verify_checksum(tmp_path: Path, requests_mock):
    """Test the checksum verification logic."""
    # Create a dummy file
    dummy_file = tmp_path / "chembl_34_postgresql.tar.gz"
    dummy_content = b"chembl data"
    dummy_file.write_bytes(dummy_content)

    # Mock the checksums.txt download
    _, checksums_url = downloader.get_chembl_file_urls(34)

    # Correct checksum
    hasher = hashlib.md5()
    hasher.update(dummy_content)
    correct_checksum = hasher.hexdigest()
    correct_checksums_txt = f"{correct_checksum}  {dummy_file.name}"
    requests_mock.get(checksums_url, text=correct_checksums_txt)
    assert downloader.verify_checksum(dummy_file, checksums_url) is True

    # Incorrect checksum
    incorrect_checksums_txt = f"aaaaaaaaaaaaaaaa  {dummy_file.name}"
    requests_mock.get(checksums_url, text=incorrect_checksums_txt)
    assert downloader.verify_checksum(dummy_file, checksums_url) is False
