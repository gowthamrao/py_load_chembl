from typer.testing import CliRunner

from py_load_chembl.cli import app

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
