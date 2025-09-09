import pytest
import requests
from py_load_chembl import downloader

# The base URL from the downloader module
BASE_URL = downloader.BASE_URL


def test_get_latest_chembl_version_success(requests_mock):
    """
    Tests that get_latest_chembl_version successfully finds the highest version number
    from the mocked HTML response.
    """
    # Mock the HTML response from the ChEMBL releases page
    mock_html = """
    <html><body>
    <a href="chembl_32/">chembl_32/</a>
    <a href="chembl_33/">chembl_33/</a>
    <a href="chembl_31/">chembl_31/</a>
    <a href="chembl_33.1/">chembl_33.1/</a> <!-- Should be ignored by regex -->
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


def test_download_file(requests_mock, tmp_path):
    """
    Tests that a file is successfully downloaded.
    """
    url = "http://test.com/testfile.txt"
    mock_content = b"This is a test file."
    requests_mock.get(url, content=mock_content)

    output_path = downloader.download_file(url, tmp_path)

    assert output_path.exists()
    assert output_path.name == "testfile.txt"
    assert output_path.read_bytes() == mock_content


def test_download_file_resume(requests_mock, tmp_path):
    """
    Tests that a download is resumed if a partial file already exists.
    """
    url = "http://test.com/testfile.txt"
    initial_content = b"This is the first part."
    resume_content = b" This is the second part."
    full_content = initial_content + resume_content

    # Create a partial file
    output_dir = tmp_path
    partial_file = output_dir / "testfile.txt"
    partial_file.write_bytes(initial_content)

    # Mock the server to expect a Range header and return the rest of the content
    matcher = requests_mock.get(url, content=resume_content)

    downloader.download_file(url, output_dir)

    assert matcher.call_count == 1
    assert "Range" in matcher.last_request.headers
    assert matcher.last_request.headers["Range"] == f"bytes={len(initial_content)}-"
    assert partial_file.read_bytes() == full_content


def test_verify_checksum_valid(requests_mock, tmp_path):
    """
    Tests that checksum verification passes for a valid file.
    """
    checksum_url = "http://test.com/checksums.txt"
    file_path = tmp_path / "test.zip"
    file_path.write_text("content")  # MD5 is 9a0364b9e99bb480dd25e1f0284c8555

    mock_checksum_text = "9a0364b9e99bb480dd25e1f0284c8555  test.zip"
    requests_mock.get(checksum_url, text=mock_checksum_text)

    is_valid = downloader.verify_checksum(file_path, checksum_url)
    assert is_valid is True


def test_verify_checksum_invalid(requests_mock, tmp_path):
    """
    Tests that checksum verification fails for an invalid file.
    """
    checksum_url = "http://test.com/checksums.txt"
    file_path = tmp_path / "test.zip"
    file_path.write_text("content")

    mock_checksum_text = "invalidchecksum  test.zip"
    requests_mock.get(checksum_url, text=mock_checksum_text)

    is_valid = downloader.verify_checksum(file_path, checksum_url)
    assert is_valid is False


def test_verify_checksum_file_not_in_list(requests_mock, tmp_path):
    """
    Tests that a ValueError is raised if the file is not in the checksums list.
    """
    checksum_url = "http://test.com/checksums.txt"
    file_path = tmp_path / "test.zip"
    file_path.write_text("content")

    mock_checksum_text = "checksum  other_file.zip"
    requests_mock.get(checksum_url, text=mock_checksum_text)

    with pytest.raises(ValueError, match="Could not find checksum for"):
        downloader.verify_checksum(file_path, checksum_url)
