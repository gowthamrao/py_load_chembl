from urllib.parse import urlparse
from py_load_chembl.adapters.base import DatabaseAdapter
from py_load_chembl.adapters.postgres import PostgresAdapter


def get_adapter(connection_string: str) -> DatabaseAdapter:
    """
    Factory function that returns the appropriate database adapter based on the
    connection string's scheme.

    Args:
        connection_string: The database connection string.

    Returns:
        An instance of a DatabaseAdapter subclass.

    Raises:
        NotImplementedError: If the scheme is not supported.
    """
    scheme = urlparse(connection_string).scheme
    if scheme == "postgresql":
        return PostgresAdapter(connection_string)
    # Add other adapters here in the future
    # elif scheme == "redshift":
    #     return RedshiftAdapter(connection_string)
    else:
        raise NotImplementedError(f"Database scheme '{scheme}' is not supported.")
