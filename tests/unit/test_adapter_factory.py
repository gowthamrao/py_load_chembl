import pytest
from py_load_chembl.adapters.factory import get_adapter
from py_load_chembl.adapters.postgres import PostgresAdapter


def test_get_adapter_returns_postgres_adapter():
    """
    Tests that the factory returns a PostgresAdapter for a postgresql connection string.
    """
    adapter = get_adapter("postgresql://user:pass@host/db")
    assert isinstance(adapter, PostgresAdapter)


def test_get_adapter_raises_for_unsupported_scheme():
    """
    Tests that the factory raises a NotImplementedError for an unsupported scheme.
    """
    with pytest.raises(
        NotImplementedError, match="Database scheme 'mysql' is not supported."
    ):
        get_adapter("mysql://user:pass@host/db")


def test_get_adapter_raises_for_invalid_string():
    """
    Tests that the factory raises an error for a malformed connection string.
    """
    with pytest.raises(
        NotImplementedError, match="Database scheme '' is not supported."
    ):
        get_adapter("not_a_real_connection_string")
