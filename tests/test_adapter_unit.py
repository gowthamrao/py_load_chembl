import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
import pytest

from py_load_chembl.adapters.postgres import PostgresAdapter


class TestPostgresAdapterUnit(unittest.TestCase):
    @patch("shutil.which", return_value=True)
    @patch("tempfile.TemporaryDirectory")
    @patch("tarfile.open")
    @patch("subprocess.run")
    @patch.object(PostgresAdapter, "execute_sql")
    def test_run_pg_restore_with_table_list(
        self,
        mock_execute_sql,
        mock_subprocess_run,
        mock_tarfile_open,
        mock_temp_dir,
        mock_shutil_which,
    ):
        """
        Tests that _run_pg_restore correctly constructs the pg_restore command
        with --table arguments when a table_list is provided.
        """
        # --- Setup Mocks ---
        # Mock the context manager for TemporaryDirectory
        mock_temp_dir.return_value.__enter__.return_value = "/fake/temp/dir"

        # Mock the Path object's behavior.
        mock_path_instance = MagicMock()
        mock_dir_in_list = MagicMock()
        mock_dir_in_list.is_dir.return_value = True
        mock_path_instance.iterdir.return_value = [mock_dir_in_list]

        # Patch the Path class in the adapter's module
        with patch(
            "py_load_chembl.adapters.postgres.Path", return_value=mock_path_instance
        ):
            # --- Setup Adapter and arguments ---
            adapter = PostgresAdapter("postgresql://user:pass@host/db")
            dump_path = Path("/fake/dump.tar.gz")
            table_list = ["molecule_dictionary", "compound_structures"]

            # Configure subprocess.run mock to simulate success
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_subprocess_run.return_value = mock_process

            # --- Call the method under test ---
            adapter._run_pg_restore(dump_path, schema="public", table_list=table_list)

            # --- Assertions ---
            self.assertTrue(mock_subprocess_run.called)

            # Get the args passed to subprocess.run
            call_args, call_kwargs = mock_subprocess_run.call_args
            command_list = call_args[0]

            # Check for the presence of the --table flags and their arguments
            self.assertIn("--table", command_list)
            self.assertIn("molecule_dictionary", command_list)
            self.assertIn("compound_structures", command_list)

            # Check the structure of the command
            # Example: ['pg_restore', ..., '--table', 'molecule_dictionary', '--table', 'compound_structures', ...]
            mol_dict_index = command_list.index("molecule_dictionary")
            self.assertEqual(command_list[mol_dict_index - 1], "--table")

            comp_struct_index = command_list.index("compound_structures")
            self.assertEqual(command_list[comp_struct_index - 1], "--table")

    @patch("shutil.which", return_value=True)
    @patch("tempfile.TemporaryDirectory")
    @patch("tarfile.open")
    @patch("subprocess.run")
    @patch.object(PostgresAdapter, "execute_sql")
    def test_run_pg_restore_without_table_list(
        self,
        mock_execute_sql,
        mock_subprocess_run,
        mock_tarfile_open,
        mock_temp_dir,
        mock_shutil_which,
    ):
        """
        Tests that _run_pg_restore does NOT add --table arguments when table_list is None.
        """
        mock_temp_dir.return_value.__enter__.return_value = "/fake/temp/dir"

        # Mock the Path object's behavior.
        mock_path_instance = MagicMock()
        mock_dir_in_list = MagicMock()
        mock_dir_in_list.is_dir.return_value = True
        mock_path_instance.iterdir.return_value = [mock_dir_in_list]

        with patch(
            "py_load_chembl.adapters.postgres.Path", return_value=mock_path_instance
        ):
            adapter = PostgresAdapter("postgresql://user:pass@host/db")
            dump_path = Path("/fake/dump.tar.gz")

            # Configure subprocess.run mock to simulate success
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_subprocess_run.return_value = mock_process

            adapter._run_pg_restore(dump_path, schema="public", table_list=None)

            self.assertTrue(mock_subprocess_run.called)
            call_args, call_kwargs = mock_subprocess_run.call_args
            command_list = call_args[0]

            self.assertNotIn("--table", command_list)

@pytest.fixture
def postgres_adapter():
    return PostgresAdapter("postgresql://user:password@host:5432/dbname")

import tarfile
import gzip

@patch("shutil.which", return_value="/usr/bin/pg_restore")
@patch("subprocess.run")
def test_run_pg_restore_failure(mock_subprocess_run, mock_shutil_which, postgres_adapter, tmp_path):
    """Test _run_pg_restore when pg_restore fails."""
    mock_subprocess_run.return_value = MagicMock(returncode=1, stdout="error", stderr="error")
    dump_path = tmp_path / "dump.tar.gz"
    # Create a valid, empty, gzipped tar file with a directory inside
    with gzip.open(dump_path, "wb") as f_gz:
        with tarfile.open(fileobj=f_gz, mode="w") as tar:
            # Add a dummy directory
            dir_info = tarfile.TarInfo(name="dummy_dir")
            dir_info.type = tarfile.DIRTYPE
            tar.addfile(dir_info)

    with pytest.raises(RuntimeError):
        postgres_adapter._run_pg_restore(dump_path)


@patch("shutil.which", return_value="/usr/bin/psql")
@patch("subprocess.run")
@patch.object(PostgresAdapter, "connect", return_value=MagicMock())
def test_run_psql_restore_failure(mock_connect, mock_subprocess_run, mock_shutil_which, postgres_adapter, tmp_path):
    """Test _run_psql_restore when psql fails."""
    mock_subprocess_run.return_value = MagicMock(returncode=1, stdout="error", stderr="error")
    dump_path = tmp_path / "dump.sql.gz"
    dump_path.touch()

    with pytest.raises(RuntimeError):
        postgres_adapter._run_psql_restore(dump_path, "public")
