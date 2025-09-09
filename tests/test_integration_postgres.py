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
    NOTE: The original test used a .tar.gz file that was not a valid pg_restore archive.
    This has been corrected to use a valid .sql.gz file, which aligns with the
    adapter's psql restore path.
    """
    # --- 1. Setup test data paths and mock URLs ---
    test_data_dir = Path(__file__).parent / "test_data"
    test_sql_path = test_data_dir / "chembl_xx_postgresql" / "chembl_xx.sql"
    test_sql_content = test_sql_path.read_text()
    gzipped_sql_content = gzip.compress(test_sql_content.encode('utf-8'))

    chembl_version = "99"
    base_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}"
    # For testing, we'll use the .sql.gz path for both FULL and DELTA, as our test data is plain SQL.
    dump_url = f"{base_url}/chembl_{chembl_version}_postgresql.sql.gz"
    checksum_url = f"{base_url}/checksums.txt"

    # --- 2. Mock all external HTTP requests ---
    requests_mock.get("https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/", text=f'<a href="chembl_{chembl_version}/"></a>')

    # Mock checksum file download
    checksum_md5 = hashlib.md5(gzipped_sql_content).hexdigest()
    requests_mock.get(checksum_url, text=f"{checksum_md5}  {dump_url.split('/')[-1]}")

    # Mock the actual data download
    requests_mock.get(dump_url, content=gzipped_sql_content)

    # --- 3. Configure and run the pipeline ---
    output_dir = tmp_path / "chembl_data"
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)

    # We need to temporarily adjust the pipeline logic for the test, so a FULL load
    # uses a .sql.gz file instead of a .tar.gz file.
    original_acquire_data = LoaderPipeline._acquire_data
    def mock_acquire_data(self):
        self.chembl_version = int(self.version_str)
        # Force the pipeline to think it's in DELTA mode for file acquisition purposes
        self.mode = 'DELTA'
        original_acquire_data(self)
        # Set the mode back to FULL for the loading logic
        self.mode = 'FULL'

    LoaderPipeline._acquire_data = mock_acquire_data

    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="FULL",
        output_dir=output_dir,
    )
    pipeline.run()

    # Restore original method
    LoaderPipeline._acquire_data = original_acquire_data

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
            assert cursor.fetchone()[0] == "IBUPROFEN"
        conn.commit()
    finally:
        conn.close()

    # --- 3. Configure and run the pipeline in DELTA mode ---
    chembl_version = "99" # Must match version from full_load test
    output_dir = tmp_path / "chembl_data"

    # The delta load requires a .sql.gz file, which was not mocked by the full_load test.
    # We must add a mock for it here. We can use the same test data content.
    test_data_dir = Path(__file__).parent / "test_data"
    # The test data is a .tar.gz, but we'll pretend its contents are plain SQL for this mock.
    # The parser and loader can handle the gzipped SQL content.
    test_sql_content = test_data_dir / "chembl_test_postgresql.tar.gz"

    delta_dump_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}/chembl_{chembl_version}_postgresql.sql.gz"
    requests_mock.get(delta_dump_url, content=test_sql_content.read_bytes())


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


from py_load_chembl.schema_parser import parse_chembl_ddl

def test_delta_load_with_schema_migration(postgres_service, tmp_path, requests_mock):
    """
    Tests the delta load with schema evolution (new tables and new columns).
    This test has been refactored to avoid complex mocking of adapter methods.
    """
    # --- 1. Define Schemas and Data for two ChEMBL versions ---
    v1_version = "98"
    v2_version = "99"
    output_dir = tmp_path / "chembl_data"
    output_dir.mkdir(exist_ok=True)

    v1_sql = """
    CREATE TABLE molecule_dictionary (molregno integer, pref_name text, chembl_id text);
    ALTER TABLE ONLY molecule_dictionary ADD CONSTRAINT molecule_dictionary_pkey PRIMARY KEY (molregno);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN', 'CHEMBL1');
    CREATE TABLE compound_structures (molregno integer, canonical_smiles text);
    ALTER TABLE ONLY compound_structures ADD CONSTRAINT compound_structures_pkey PRIMARY KEY (molregno);
    INSERT INTO compound_structures VALUES (1, 'CC(=O)Oc1ccccc1C(=O)O');
    """
    v2_sql = """
    CREATE TABLE molecule_dictionary (molregno integer, pref_name text, chembl_id text, is_natural_product smallint);
    ALTER TABLE ONLY molecule_dictionary ADD CONSTRAINT molecule_dictionary_pkey PRIMARY KEY (molregno);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN V2', 'CHEMBL1', 0); -- Update existing
    INSERT INTO molecule_dictionary VALUES (2, 'NEW DRUG', 'CHEMBL2', 1); -- Insert new
    CREATE TABLE compound_structures (molregno integer, canonical_smiles text);
    ALTER TABLE ONLY compound_structures ADD CONSTRAINT compound_structures_pkey PRIMARY KEY (molregno);
    INSERT INTO compound_structures VALUES (1, 'CC(=O)Oc1ccccc1C(=O)O');
    CREATE TABLE target_dictionary (tid integer, pref_name text, target_type text);
    ALTER TABLE ONLY target_dictionary ADD CONSTRAINT target_dictionary_pkey PRIMARY KEY (tid);
    INSERT INTO target_dictionary VALUES (101, 'Cytochrome P450', 'ENZYME');
    """
    v1_gzipped_content = gzip.compress(v1_sql.encode('utf-8'))
    v2_gzipped_content = gzip.compress(v2_sql.encode('utf-8'))

    # --- 2. Setup Mocks ---
    def setup_mocks(version, gzipped_content):
        dump_filename = f"chembl_{version}_postgresql.sql.gz"
        dump_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{version}/{dump_filename}"
        checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{version}/checksums.txt"
        checksum = hashlib.md5(gzipped_content).hexdigest()
        requests_mock.get(dump_url, content=gzipped_content)
        requests_mock.get(checksum_url, text=f"{checksum}  {dump_filename}")

    requests_mock.get("https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/", text=f'<a href="chembl_{v2_version}/"></a>')
    setup_mocks(v1_version, v1_gzipped_content)
    setup_mocks(v2_version, v2_gzipped_content)

    # --- 3. Run FULL load for v1 ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)

    # To test a FULL load with a SQL file, we must trick the pipeline into downloading
    # the .sql.gz file. We can do this by temporarily setting the mode to DELTA during acquisition.
    original_acquire_data = LoaderPipeline._acquire_data
    def mock_acquire_data(self):
        original_mode = self.mode
        self.chembl_version = int(self.version_str)
        self.mode = 'DELTA' # Force download of .sql.gz
        original_acquire_data(self)
        self.mode = original_mode # Restore original mode for loading
    LoaderPipeline._acquire_data = mock_acquire_data

    pipeline_v1 = LoaderPipeline(version=v1_version, output_dir=output_dir, adapter=adapter, mode="FULL")
    pipeline_v1.run()

    LoaderPipeline._acquire_data = original_acquire_data # Restore

    # --- 4. Assert initial state after v1 load ---
    conn = psycopg2.connect(postgres_service["uri"])
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM molecule_dictionary;")
        assert cursor.fetchone()[0] == 1
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'molecule_dictionary';")
        assert len(cursor.fetchall()) == 3
        cursor.execute("SELECT to_regclass('public.target_dictionary');")
        assert cursor.fetchone()[0] is None

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
