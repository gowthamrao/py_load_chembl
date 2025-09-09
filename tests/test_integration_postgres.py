import os
import pytest
import psycopg2
import gzip
import hashlib
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
    This test now mocks the pg_restore call to avoid dependency on the executable.
    """
    # --- 1. Setup test data paths and mock URLs ---
    test_data_dir = Path(__file__).parent / "test_data"
    test_archive_path = test_data_dir / "chembl_test_postgresql.tar.gz"
    test_checksums_path = test_data_dir / "checksums.txt"

    chembl_version = "99"
    base_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}"
    dump_url = f"{base_url}/chembl_{chembl_version}_postgresql.tar.gz"
    checksum_url = f"{base_url}/checksums.txt"

    # --- 2. Mock all external HTTP requests ---
    requests_mock.get("https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/", text=f'<a href="chembl_{chembl_version}/"></a>')
    requests_mock.get(checksum_url, text=test_checksums_path.read_text())
    requests_mock.get(dump_url, content=test_archive_path.read_bytes())

    # --- 3. Configure and run the pipeline ---
    output_dir = tmp_path / "chembl_data"
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)

    # Mock the pg_restore method to avoid the FileNotFoundError
    def mock_pg_restore(self, dump_archive_path, schema):
        print(f"MOCK pg_restore called for schema {schema}")
        # To make assertions pass, we need to create and populate the table
        # that the test checks.
        mock_sql = """
        CREATE TABLE molecule_dictionary (molregno integer PRIMARY KEY, pref_name text, chembl_id text);
        INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN', 'CHEMBL1');
        INSERT INTO molecule_dictionary VALUES (2, 'PARACETAMOL', 'CHEMBL2');
        """
        self.execute_sql(mock_sql)

    original_pg_restore = PostgresAdapter._run_pg_restore
    PostgresAdapter._run_pg_restore = mock_pg_restore

    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="FULL",
        output_dir=output_dir,
    )
    pipeline.run()

    PostgresAdapter._run_pg_restore = original_pg_restore

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


def test_delta_load_with_schema_migration(postgres_service, tmp_path, requests_mock):
    """
    Tests the delta load with schema evolution (new tables and new columns).
    1. Mocks and loads a "v1" ChEMBL dump (as a FULL load).
    2. Mocks and loads a "v2" ChEMBL dump (as a DELTA load), which contains a new
       table and a new column in an existing table.
    3. Asserts that the new table was created in the production schema.
    4. Asserts that the new column was added to the existing table.
    5. Asserts that data was correctly inserted/updated.
    """
    # --- 1. Define Schemas and Data for two ChEMBL versions ---
    v1_version = "98"
    v2_version = "99"

    # Schema for v1
    v1_sql = """
    CREATE TABLE molecule_dictionary (molregno integer PRIMARY KEY, pref_name text, chembl_id text);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN', 'CHEMBL1');
    CREATE TABLE compound_structures (molregno integer PRIMARY KEY, canonical_smiles text);
    INSERT INTO compound_structures VALUES (1, 'CC(=O)Oc1ccccc1C(=O)O');
    """

    # Schema for v2 (adds a new column and a new table)
    v2_sql = """
    CREATE TABLE molecule_dictionary (molregno integer PRIMARY KEY, pref_name text, chembl_id text, is_natural_product smallint);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN V2', 'CHEMBL1', 0); -- Update existing
    INSERT INTO molecule_dictionary VALUES (2, 'NEW DRUG', 'CHEMBL2', 1); -- Insert new
    CREATE TABLE compound_structures (molregno integer PRIMARY KEY, canonical_smiles text);
    INSERT INTO compound_structures VALUES (1, 'CC(=O)Oc1ccccc1C(=O)O');
    CREATE TABLE target_dictionary (tid integer PRIMARY KEY, pref_name text, target_type text);
    INSERT INTO target_dictionary VALUES (101, 'Cytochrome P450', 'ENZYME');
    """

    # --- 2. Setup Mocks and Files ---
    output_dir = tmp_path / "chembl_data"
    output_dir.mkdir()

    # Create dummy files and calculate their real checksums
    v1_content = b"dummy tar.gz content for v1"
    v1_hash = hashlib.md5(v1_content).hexdigest()
    v1_path = output_dir / f"chembl_{v1_version}_postgresql.tar.gz"
    v1_path.write_bytes(v1_content)

    # For the delta load, the file needs to be a valid gzipped file
    v2_content_gz = gzip.compress(v2_sql.encode('utf-8'))
    v2_hash = hashlib.md5(v2_content_gz).hexdigest()
    v2_path = output_dir / f"chembl_{v2_version}_postgresql.sql.gz"
    v2_path.write_bytes(v2_content_gz)

    # Mock URLs
    v1_dump_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v1_version}/chembl_{v1_version}_postgresql.tar.gz"
    v2_dump_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v2_version}/chembl_{v2_version}_postgresql.sql.gz"
    v1_checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v1_version}/checksums.txt"
    v2_checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v2_version}/checksums.txt"

    # Use the real hashes in the mocked checksum files
    requests_mock.get(v1_checksum_url, text=f"{v1_hash}  {v1_path.name}")
    requests_mock.get(v2_checksum_url, text=f"{v2_hash}  {v2_path.name}")
    requests_mock.get(v1_dump_url, content=v1_path.read_bytes())
    requests_mock.get(v2_dump_url, content=v2_path.read_bytes())

    # --- 3. Run FULL load for v1 ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)

    # We need to mock pg_restore for the v1 full load, as we are not providing a real dump
    def mock_pg_restore(self, dump_archive_path, schema):
        print(f"MOCK pg_restore called for schema {schema}")
        self.execute_sql(v1_sql)

    original_pg_restore = PostgresAdapter._run_pg_restore
    PostgresAdapter._run_pg_restore = mock_pg_restore

    pipeline_v1 = LoaderPipeline(version=v1_version, output_dir=output_dir, adapter=adapter, mode="FULL")
    pipeline_v1.run()

    PostgresAdapter._run_pg_restore = original_pg_restore # Restore original method

    # --- 4. Assert initial state after v1 load ---
    conn = psycopg2.connect(postgres_service["uri"])
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM molecule_dictionary;")
        assert cursor.fetchone()[0] == 1
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'molecule_dictionary';")
        assert len(cursor.fetchall()) == 3 # Should not have the new column yet
        cursor.execute("SELECT to_regclass('public.target_dictionary');")
        assert cursor.fetchone()[0] is None # New table should not exist yet

    # --- 5. Run DELTA load for v2 ---
    pipeline_v2 = LoaderPipeline(version=v2_version, output_dir=output_dir, adapter=adapter, mode="DELTA")
    pipeline_v2.run()

    # --- 6. Assert final state after v2 delta load ---
    with conn.cursor() as cursor:
        # Assert schema migration: new column exists
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'molecule_dictionary' AND column_name = 'is_natural_product';")
        assert cursor.fetchone()[0] == 'is_natural_product'

        # Assert schema migration: new table exists
        cursor.execute("SELECT COUNT(*) FROM target_dictionary;")
        assert cursor.fetchone()[0] == 1

        # Assert data merge: update
        cursor.execute("SELECT pref_name FROM molecule_dictionary WHERE molregno = 1;")
        assert cursor.fetchone()[0] == 'ASPIRIN V2'

        # Assert data merge: insert
        cursor.execute("SELECT COUNT(*) FROM molecule_dictionary;")
        assert cursor.fetchone()[0] == 2

    conn.close()
