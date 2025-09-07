import os
import pytest
import psycopg2
from pathlib import Path
import requests_mock

from py_load_chembl.pipeline import LoaderPipeline
from py_load_chembl.adapters.postgres import PostgresAdapter

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration

# Helper function to check if docker is running
def is_docker_running():
    import subprocess
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False

# Skip all tests in this file if Docker is not running
if not is_docker_running():
    pytest.skip("Docker is not running, skipping integration tests.", allow_module_level=True)


@pytest.fixture(scope="module")
def postgres_service(docker_ip, docker_services):
    """
    Fixture to get the connection details for the running PostgreSQL container.
    This assumes you have a docker-compose.yml with a 'postgres' service.
    """
    port = docker_services.port_for("postgres", 5432)
    host = docker_ip
    user = "testuser"
    password = "testpassword"
    dbname = "testdb"

    # Wait for the postgres service to be ready
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.5, check=lambda: is_postgres_ready(host, port, user, password, dbname)
    )

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": dbname,
        "uri": f"postgresql://{user}:{password}@{host}:{port}/{dbname}",
    }


def is_postgres_ready(host, port, user, password, dbname):
    """Check if the postgres container is ready to accept connections."""
    try:
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=dbname, connect_timeout=2
        )
        conn.close()
        return True
    except psycopg2.OperationalError:
        return False


def test_postgres_full_load(postgres_service, tmp_path, requests_mock):
    """
    Tests the end-to-end FULL load process against a containerized PostgreSQL.
    """
    # --- 1. Setup test data paths and mock URLs ---
    test_data_dir = Path(__file__).parent / "test_data"
    test_archive_path = test_data_dir / "chembl_test_postgresql.tar.gz"
    test_checksums_path = test_data_dir / "checksums.txt"

    chembl_version = "99"
    base_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}"
    dump_url = f"{base_url}/chembl_test_postgresql.tar.gz"
    checksum_url = f"{base_url}/checksums.txt"

    # --- 2. Mock all external HTTP requests ---
    # Mock latest version discovery
    requests_mock.get("https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/", text=f'<a href="chembl_{chembl_version}/"></a>')
    # Mock checksum file download
    requests_mock.get(checksum_url, text=test_checksums_path.read_text())
    # Mock the actual data download
    requests_mock.get(dump_url, content=test_archive_path.read_bytes())

    # --- 3. Configure and run the pipeline ---
    output_dir = tmp_path / "chembl_data"
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])

    # Before running the main pipeline, ensure the database exists.
    # In a real scenario, this would be a manual step or done by an admin.
    # For the test, we connect to 'postgres' db to create our test db.
    _create_database(postgres_service)

    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="FULL",
        output_dir=output_dir,
    )
    pipeline.run()

    # --- 4. Assert the results ---
    conn = None
    try:
        conn = psycopg2.connect(postgres_service["uri"])
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM molecule_dictionary;")
            count = cursor.fetchone()[0]
            assert count == 2

            cursor.execute("SELECT chembl_id, pref_name FROM molecule_dictionary WHERE molregno = 1;")
            chembl_id, pref_name = cursor.fetchone()
            assert chembl_id == "CHEMBL1"
            assert pref_name == "ASPIRIN"
    finally:
        if conn:
            conn.close()

def _create_database(pg_config):
    """Creates the test database if it doesn't exist."""
    # Connect to the default 'postgres' database to run the CREATE DATABASE command
    conn = psycopg2.connect(
        host=pg_config["host"],
        port=pg_config["port"],
        user=pg_config["user"],
        password=pg_config["password"],
        dbname="postgres" # Default db that always exists
    )
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    with conn.cursor() as cursor:
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{pg_config['dbname']}'")
        exists = cursor.fetchone()
        if not exists:
            print(f"Database '{pg_config['dbname']}' does not exist. Creating it...")
            cursor.execute(f"CREATE DATABASE {pg_config['dbname']}")
        else:
            print(f"Database '{pg_config['dbname']}' already exists.")
            # Clean up tables from previous runs for idempotency
            _cleanup_db(pg_config)

    conn.close()

def _cleanup_db(pg_config):
    """Connects to the test DB and drops all public tables."""
    try:
        conn = psycopg2.connect(pg_config["uri"])
        with conn.cursor() as cursor:
            # Drop all tables in the public schema
            cursor.execute("""
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
            """)
        conn.commit()
        conn.close()
        print(f"Cleaned up tables in database '{pg_config['dbname']}'.")
    except psycopg2.OperationalError:
        # This can happen if the DB doesn't exist yet, which is fine.
        pass
