import pytest
import requests
from py_load_chembl import downloader
from py_load_chembl.downloader import ChecksumError

# The base URL from the downloader module
BASE_URL = downloader.BASE_URL


@pytest.fixture
def mock_downloader(requests_mock, tmp_path):
    """Fixture to mock all external calls made by the downloader."""

    def _mock(
        version=33,
        file_content=b"test content",
        checksum_valid=True,
        plain_sql=False,
    ):
        # Mock version detection if needed
        requests_mock.get(
            f"{BASE_URL}/", text=f'<a href="chembl_{version}/">chembl_{version}/</a>'
        )

        # Mock file URLs
        if plain_sql:
            file_name = f"chembl_{version}_postgresql.sql.gz"
        else:
            file_name = f"chembl_{version}_postgresql.tar.gz"

        dump_url = f"{BASE_URL}/chembl_{version}/{file_name}"
        checksums_url = f"{BASE_URL}/chembl_{version}/checksums.txt"

        # Mock file download
        requests_mock.get(dump_url, content=file_content)

        # Mock checksum file download
        if checksum_valid:
            actual_checksum = downloader.hashlib.md5(file_content).hexdigest()
        else:
            actual_checksum = "invalidchecksum"
        checksum_text = f"{actual_checksum}  {file_name}"
        requests_mock.get(checksums_url, text=checksum_text)

        return {
            "version": version,
            "output_dir": tmp_path,
            "file_name": file_name,
            "plain_sql": plain_sql,
        }

    return _mock


def test_download_and_verify_success(mock_downloader):
    """
    Tests the end-to-end success case where a file is downloaded and its checksum is valid.
    """
    mock_params = mock_downloader(file_content=b"good data", checksum_valid=True)

    output_path = downloader.download_chembl_db(
        version=mock_params["version"],
        output_dir=mock_params["output_dir"],
        plain_sql=mock_params["plain_sql"],
    )

    # Assert that the file was downloaded and exists
    assert output_path.exists()
    assert output_path.name == mock_params["file_name"]
    assert output_path.read_bytes() == b"good data"


def test_download_and_verify_checksum_error(mock_downloader):
    """
    Tests that a ChecksumError is raised and the corrupted file is deleted when
    the checksum is invalid.
    """
    mock_params = mock_downloader(file_content=b"bad data", checksum_valid=False)
    output_dir = mock_params["output_dir"]
    file_path = output_dir / mock_params["file_name"]

    with pytest.raises(
        ChecksumError, match="is invalid. The file has been deleted."
    ):
        downloader.download_chembl_db(
            version=mock_params["version"],
            output_dir=output_dir,
            plain_sql=mock_params["plain_sql"],
        )

    # Assert that the corrupted file was deleted
    assert not file_path.exists()


def test_get_latest_chembl_version_success(requests_mock):
    """
    Tests that get_latest_chembl_version successfully finds the highest version number
    from the mocked HTML response.
    """
    mock_html = """
    <html><body>
    <a href="chembl_32/">chembl_32/</a>
    <a href="chembl_33/">chembl_33/</a>
    <a href="chembl_31/">chembl_31/</a>
    </body></html>
    """
    requests_mock.get(f"{BASE_URL}/", text=mock_html)

    latest_version = downloader.get_latest_chembl_version()
    assert latest_version == 33


def test_get_latest_chembl_version_no_versions_found(requests_mock):
    """
    Tests that a ValueError is raised if no version links are found in the HTML.
    """
    requests_mock.get(
        f"{BASE_URL}/", text="<html><body>No versions here.</body></html>"
    )

    with pytest.raises(ValueError, match="Could not find any ChEMBL versions"):
        downloader.get_latest_chembl_version()


def test_get_latest_chembl_version_connection_error(requests_mock):
    """
    Tests that a ConnectionError is raised if the request fails.
    """
    requests_mock.get(
        f"{BASE_URL}/",
        exc=requests.exceptions.RequestException("Test connection error"),
    )

    with pytest.raises(ConnectionError, match="Could not connect to ChEMBL FTP server"):
        downloader.get_latest_chembl_version()


def test_get_chembl_file_urls():
    """
    Tests the construction of download URLs for different file types.
    """
    version = 33

    # Test for pg_restore format (.tar.gz)
    dump_url, checksums_url = downloader.get_chembl_file_urls(version, plain_sql=False)
    assert dump_url == f"{BASE_URL}/chembl_33/chembl_33_postgresql.tar.gz"
    assert checksums_url == f"{BASE_URL}/chembl_33/checksums.txt"

    # Test for plain SQL format (.sql.gz)
    dump_url, checksums_url = downloader.get_chembl_file_urls(version, plain_sql=True)
    assert dump_url == f"{BASE_URL}/chembl_33/chembl_33_postgresql.sql.gz"
    assert checksums_url == f"{BASE_URL}/chembl_33/checksums.txt"
