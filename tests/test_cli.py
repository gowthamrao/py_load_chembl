from typer.testing import CliRunner
from unittest.mock import patch

from py_load_chembl.cli import app
from py_load_chembl.config import STANDARD_TABLE_SUBSET

runner = CliRunner()


def test_app_help():
    """Test the --help message for the CLI."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.stdout


def test_load_command_help():
    """Test the --help message for the load command."""
    result = runner.invoke(app, ["load", "--help"])
    assert result.exit_code == 0
    assert "Downloads and loads ChEMBL data" in result.stdout
    assert "--target" in result.stdout
    assert "--mode" in result.stdout
    assert "--representation" in result.stdout


@patch('py_load_chembl.cli.api.full_load')
def test_load_command_with_standard_representation(mock_full_load):
    """Tests that using --representation standard calls the API with the correct table list."""
    result = runner.invoke(
        app,
        [
            "load",
            "--target", "postgresql://fake",
            "--representation", "standard",
        ],
    )
    assert result.exit_code == 0
    mock_full_load.assert_called_once()
    # Check the keyword arguments passed to the mocked function
    call_args, call_kwargs = mock_full_load.call_args
    assert call_kwargs["include_tables"] == STANDARD_TABLE_SUBSET


@patch('py_load_chembl.cli.api.full_load')
def test_load_command_with_full_representation(mock_full_load):
    """Tests that the default 'full' representation calls the API with no table list."""
    result = runner.invoke(
        app,
        [
            "load",
            "--target", "postgresql://fake",
            # No --representation flag, should default to 'full'
        ],
    )
    assert result.exit_code == 0
    mock_full_load.assert_called_once()
    # Check the keyword arguments passed to the mocked function
    call_args, call_kwargs = mock_full_load.call_args
    assert call_kwargs["include_tables"] is None
