import os
import pytest
import psycopg2
from pathlib import Path
import requests_mock

from py_load_chembl.pipeline import LoaderPipeline
from py_load_chembl.adapters.postgres import PostgresAdapter

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration

# The pytest-docker plugin handles service availability.
# The manual check below is removed as it can fail in certain CI environments
# where the docker socket is available but the CLI is not in the path.

# @pytest.fixture(scope="module")
# def is_docker_running():
#     ...
# if not is_docker_running():
#     ...

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


def test_postgres_delta_load_workflow(postgres_service, tmp_path, requests_mock):
    """
    Tests the new, robust delta load workflow.
    1. Runs a full load to establish an initial "production" state.
    2. Manually alters a record in the production database.
    3. Runs a delta load with the same source data.
    4. Asserts that the altered record was merged (updated) correctly.
    5. Asserts that the temporary staging schema was cleaned up.
    """
    # --- 1. Setup: Run a FULL load to create the initial state ---
    # This uses the exact same logic and mocks as the full_load test
    test_postgres_full_load(postgres_service, tmp_path, requests_mock)

    # --- 2. Setup: Manually alter a record in the "production" database ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            # This record will be updated by the delta load
            cursor.execute("UPDATE molecule_dictionary SET pref_name = 'OLD ASPIRIN' WHERE molregno = 1;")
            # This record should not be touched
            cursor.execute("SELECT pref_name FROM molecule_dictionary WHERE molregno = 2;")
            assert cursor.fetchone()[0] == "PARACETAMOL"
        conn.commit()
    finally:
        conn.close()

    # --- 3. Configure and run the pipeline in DELTA mode ---
    # Mocks are already set up from the full load test call
    chembl_version = "99" # Must match version from full_load test
    output_dir = tmp_path / "chembl_data"
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="DELTA",
        output_dir=output_dir,
    )
    pipeline.run()

    # --- 4. Assert the final state of the database ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            # Assert that the record was updated back to its original name
            cursor.execute("SELECT pref_name FROM molecule_dictionary WHERE molregno = 1;")
            assert cursor.fetchone()[0] == "ASPIRIN"

            # Assert that the other record was not changed
            cursor.execute("SELECT pref_name FROM molecule_dictionary WHERE molregno = 2;")
            assert cursor.fetchone()[0] == "PARACETAMOL"

            # Assert that no new records were added
            cursor.execute("SELECT COUNT(*) FROM molecule_dictionary;")
            assert cursor.fetchone()[0] == 2

            # Assert that the staging schema was dropped
            staging_schema = f"staging_chembl_{chembl_version}"
            cursor.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s;", (staging_schema,))
            assert cursor.fetchone() is None, f"Staging schema '{staging_schema}' was not dropped."

            # Assert the metadata tables for the DELTA load
            cursor.execute("SELECT insert_count, update_count FROM chembl_loader_meta.load_details WHERE load_id = %s AND table_name = 'molecule_dictionary';", (pipeline.load_id,))
            insert_count, update_count = cursor.fetchone()
            assert insert_count == 0  # No new records were inserted
            assert update_count == 1  # The 'OLD ASPIRIN' record was updated
    finally:
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
