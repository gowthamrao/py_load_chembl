import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

from py_load_chembl import api


class TestApi(unittest.TestCase):
    @patch("py_load_chembl.api.LoaderPipeline")
    @patch("py_load_chembl.api.PostgresAdapter")
    def test_full_load_api_call(self, mock_adapter, mock_pipeline):
        """Tests that the full_load API function initializes and runs the pipeline correctly."""
        mock_pipeline_instance = MagicMock()
        mock_pipeline.return_value = mock_pipeline_instance

        api.full_load(
            connection_string="postgresql://user:pass@host/db",
            chembl_version="99",
            output_dir=Path("/tmp/test"),
            include_tables=["table1", "table2"],
        )

        mock_adapter.assert_called_once_with(
            connection_string="postgresql://user:pass@host/db"
        )

        mock_pipeline.assert_called_once_with(
            adapter=mock_adapter.return_value,
            version="99",
            mode="FULL",
            output_dir=Path("/tmp/test"),
            include_tables=["table1", "table2"],
        )

        mock_pipeline_instance.run.assert_called_once()

    @patch("py_load_chembl.api.LoaderPipeline")
    @patch("py_load_chembl.api.PostgresAdapter")
    def test_delta_load_api_call(self, mock_adapter, mock_pipeline):
        """Tests that the delta_load API function initializes and runs the pipeline correctly."""
        mock_pipeline_instance = MagicMock()
        mock_pipeline.return_value = mock_pipeline_instance

        api.delta_load(
            connection_string="postgresql://user:pass@host/db",
            chembl_version="100",
            output_dir=Path("/tmp/delta"),
            include_tables=["table3"],
        )

        mock_adapter.assert_called_once_with(
            connection_string="postgresql://user:pass@host/db"
        )

        mock_pipeline.assert_called_once_with(
            adapter=mock_adapter.return_value,
            version="100",
            mode="DELTA",
            output_dir=Path("/tmp/delta"),
            include_tables=["table3"],
        )

        mock_pipeline_instance.run.assert_called_once()

    def test_delta_load_raises_error_on_latest(self):
        """Tests that delta_load raises a ValueError if 'latest' is passed as the version."""
        with self.assertRaises(ValueError):
            api.delta_load(
                connection_string="postgresql://user:pass@host/db",
                chembl_version="latest",
            )
