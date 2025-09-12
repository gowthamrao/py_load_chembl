import pytest
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner
from py_load_chembl.cli import app
from py_load_chembl import downloader

runner = CliRunner()

@patch("py_load_chembl.cli.api.full_load")
def test_load_command_full_mode(mock_full_load):
    """Test the load command in FULL mode."""
    result = runner.invoke(app, ["load", "--target", "dummy_uri", "--mode", "FULL", "--version", "99"])
    assert result.exit_code == 0
    assert "load process completed successfully" in result.stdout
    mock_full_load.assert_called_once()

@patch("py_load_chembl.cli.api.delta_load")
def test_load_command_delta_mode(mock_delta_load):
    """Test the load command in DELTA mode."""
    result = runner.invoke(app, ["load", "--target", "dummy_uri", "--mode", "DELTA", "--version", "99"])
    assert result.exit_code == 0
    assert "load process completed successfully" in result.stdout
    mock_delta_load.assert_called_once()

def test_load_command_invalid_mode(caplog):
    """Test the load command with an invalid mode."""
    result = runner.invoke(app, ["load", "--target", "dummy_uri", "--mode", "INVALID", "--version", "99"])
    assert result.exit_code == 1
    assert "Invalid mode: INVALID" in caplog.text

@patch("py_load_chembl.cli.api.full_load", side_effect=ConnectionError("Test connection error"))
def test_load_command_full_mode_connection_error(mock_full_load, caplog):
    """Test the load command in FULL mode with a connection error."""
    result = runner.invoke(app, ["load", "--target", "dummy_uri", "--mode", "FULL", "--version", "99"])
    assert result.exit_code == 1
    assert "A critical error occurred" in caplog.text

@patch("py_load_chembl.cli.downloader.get_latest_chembl_version", return_value="99")
@patch("py_load_chembl.cli.downloader.download_chembl_db")
def test_download_command(mock_download, mock_get_latest):
    """Test the download command."""
    result = runner.invoke(app, ["download", "--version", "latest"])
    assert result.exit_code == 0
    assert "download process completed successfully" in result.stdout
    mock_download.assert_called_once()

@patch("py_load_chembl.cli.downloader.get_latest_chembl_version", side_effect=downloader.ChecksumError("Test checksum error"))
def test_download_command_checksum_error(mock_get_latest):
    """Test the download command with a checksum error."""
    result = runner.invoke(app, ["download", "--version", "latest"])
    assert result.exit_code == 1
    assert "A critical error occurred" in result.stdout

@patch("py_load_chembl.cli.api.full_load")
def test_load_command_standard_representation(mock_full_load, caplog):
    """Test the load command with standard representation."""
    result = runner.invoke(app, ["load", "--target", "dummy_uri", "--mode", "FULL", "--version", "99", "--representation", "standard"])
    assert result.exit_code == 0
    assert "Using 'standard' representation" in caplog.text
    mock_full_load.assert_called_once()
