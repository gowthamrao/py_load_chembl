import os
import pytest
import psycopg2
import gzip
import hashlib
import tempfile
import tarfile
from pathlib import Path
import requests_mock
import io
import json
import logging
from unittest.mock import patch

from py_load_chembl.pipeline import LoaderPipeline
from py_load_chembl.adapters.postgres import PostgresAdapter

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration

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


from py_load_chembl.logging_config import JsonFormatter

def test_postgres_full_load(postgres_service, tmp_path, requests_mock, caplog):
    """
    Tests the end-to-end FULL load process against a containerized PostgreSQL.
    This test now also verifies that structured logging is working correctly.
    """
    handler = caplog.handler
    handler.setFormatter(JsonFormatter())
    caplog.set_level(logging.INFO)
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

        # --- 5. Assert logging ---
        log_records = [json.loads(rec) for rec in caplog.text.strip().split('\n') if rec]

        # Check for a log from the pipeline
        pipeline_start_log = next((rec for rec in log_records if "Starting ChEMBL load" in rec["message"]), None)
        assert pipeline_start_log is not None
        assert pipeline_start_log["level"] == "INFO"
        assert pipeline_start_log["name"] == "py_load_chembl.pipeline"

        # Check for a log from the adapter
        adapter_log = next((rec for rec in log_records if "Schema-specific restore completed" in rec["message"]), None)
        assert adapter_log is not None
        assert adapter_log["level"] == "INFO"
        assert adapter_log["name"] == "py_load_chembl.adapters.postgres"

        # Check for a database logging message
        db_log = next((rec for rec in log_records if "Logging to database with load_id" in rec["message"]), None)
        assert db_log is not None
        assert db_log["level"] == "INFO"

    finally:
        if conn:
            conn.close()


def test_postgres_delta_load_workflow(postgres_service, tmp_path, requests_mock, caplog):
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
    test_postgres_full_load(postgres_service, tmp_path, requests_mock, caplog)

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
            assert cursor.fetchone()[0] == "IBUPROFEN"

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
            # The UPSERT will update all conflicting rows, even if values are the same.
            assert update_count == 2
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


def test_delta_load_with_soft_deletes(postgres_service, tmp_path, requests_mock):
    """
    Tests that the delta load correctly handles soft deletes.
    1. Load v1 with 3 records.
    2. Load v2 where one record is removed, one is updated, and one is new.
    3. Assert that the removed record is marked as is_deleted=True.
    4. Assert that the updated record is updated and is_deleted=False.
    5. Assert that the new record is inserted and is_deleted=False.
    6. Assert that the metadata log shows the correct obsolete_count.
    """
    # --- 1. Define Schemas and Data for two ChEMBL versions ---
    v1_version = "100"
    v2_version = "101"
    output_dir = tmp_path / "chembl_data"
    output_dir.mkdir(exist_ok=True)

    # Version 1: Aspirin, Ibuprofen, Paracetamol
    v1_sql = """
    CREATE TABLE molecule_dictionary (molregno integer, pref_name text, chembl_id text);
    ALTER TABLE ONLY molecule_dictionary ADD CONSTRAINT molecule_dictionary_pkey PRIMARY KEY (molregno);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN', 'CHEMBL1');
    INSERT INTO molecule_dictionary VALUES (2, 'IBUPROFEN', 'CHEMBL2');
    INSERT INTO molecule_dictionary VALUES (3, 'PARACETAMOL', 'CHEMBL3');
    """
    # Version 2: Aspirin (updated), Paracetamol (same), Celecoxib (new). Ibuprofen is now obsolete.
    v2_sql = """
    CREATE TABLE molecule_dictionary (molregno integer, pref_name text, chembl_id text);
    ALTER TABLE ONLY molecule_dictionary ADD CONSTRAINT molecule_dictionary_pkey PRIMARY KEY (molregno);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN V2', 'CHEMBL1'); -- Update
    INSERT INTO molecule_dictionary VALUES (3, 'PARACETAMOL', 'CHEMBL3'); -- No change
    INSERT INTO molecule_dictionary VALUES (4, 'CELECOXIB', 'CHEMBL4'); -- Insert
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

    # We must trick the pipeline to use .sql.gz for a FULL load for this test
    original_acquire_data = LoaderPipeline._acquire_data
    def mock_acquire_data(self):
        original_mode = self.mode
        self.chembl_version = int(self.version_str)
        self.mode = 'DELTA'
        original_acquire_data(self)
        self.mode = original_mode
    LoaderPipeline._acquire_data = mock_acquire_data

    pipeline_v1 = LoaderPipeline(version=v1_version, output_dir=output_dir, adapter=adapter, mode="FULL")
    pipeline_v1.run()
    LoaderPipeline._acquire_data = original_acquire_data # Restore

    # --- 4. Run DELTA load for v2 ---
    pipeline_v2 = LoaderPipeline(version=v2_version, output_dir=output_dir, adapter=adapter, mode="DELTA")
    pipeline_v2.run()

    # --- 5. Assert final state ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pref_name, is_deleted FROM molecule_dictionary ORDER BY molregno;")
            results = cursor.fetchall()

            # Expected: (Aspirin V2, False), (Ibuprofen, True), (Paracetamol, False), (Celecoxib, False)
            assert len(results) == 4

            # molregno=1 (Updated)
            assert results[0][0] == 'ASPIRIN V2'
            assert results[0][1] is False
            # molregno=2 (Obsolete)
            assert results[1][0] == 'IBUPROFEN'
            assert results[1][1] is True
            # molregno=3 (Unchanged)
            assert results[2][0] == 'PARACETAMOL'
            assert results[2][1] is False
            # molregno=4 (Inserted)
            assert results[3][0] == 'CELECOXIB'
            assert results[3][1] is False

            # Assert metadata log
            cursor.execute("""
                SELECT insert_count, update_count, obsolete_count
                FROM chembl_loader_meta.load_details
                WHERE load_id = %s AND table_name = 'molecule_dictionary';
            """, (pipeline_v2.load_id,))
            insert_count, update_count, obsolete_count = cursor.fetchone()
            assert insert_count == 1  # Celecoxib
            assert update_count == 2  # Aspirin (name changed) and Paracetamol (re-affirmed)
            assert obsolete_count == 1 # Ibuprofen
    finally:
        conn.close()


def test_delta_load_with_table_subset(postgres_service, tmp_path, requests_mock):
    """
    Tests that a DELTA load with `include_tables` only affects the specified tables.
    """
    # --- 1. Define Schemas and Data for two ChEMBL versions ---
    v1_version = "102"
    v2_version = "103"
    output_dir = tmp_path / "chembl_data"
    output_dir.mkdir(exist_ok=True)

    # Version 1: Contains molecule_dictionary and target_dictionary
    v1_sql = """
    CREATE TABLE molecule_dictionary (molregno integer, pref_name text, chembl_id text);
    ALTER TABLE ONLY molecule_dictionary ADD CONSTRAINT molecule_dictionary_pkey PRIMARY KEY (molregno);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN', 'CHEMBL1');

    CREATE TABLE target_dictionary (tid integer, pref_name text, target_type text);
    ALTER TABLE ONLY target_dictionary ADD CONSTRAINT target_dictionary_pkey PRIMARY KEY (tid);
    INSERT INTO target_dictionary VALUES (101, 'Cytochrome P450', 'ENZYME');
    """
    # Version 2: Contains updated data for both tables
    v2_sql = """
    CREATE TABLE molecule_dictionary (molregno integer, pref_name text, chembl_id text);
    ALTER TABLE ONLY molecule_dictionary ADD CONSTRAINT molecule_dictionary_pkey PRIMARY KEY (molregno);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN V2', 'CHEMBL1'); -- Updated

    CREATE TABLE target_dictionary (tid integer, pref_name text, target_type text);
    ALTER TABLE ONLY target_dictionary ADD CONSTRAINT target_dictionary_pkey PRIMARY KEY (tid);
    INSERT INTO target_dictionary VALUES (101, 'Cytochrome P450 V2', 'ENZYME'); -- Updated
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

    # Trick pipeline to use .sql.gz for FULL load
    original_acquire_data = LoaderPipeline._acquire_data
    def mock_acquire_data(self):
        original_mode = self.mode
        self.chembl_version = int(self.version_str)
        self.mode = 'DELTA'
        original_acquire_data(self)
        self.mode = original_mode
    LoaderPipeline._acquire_data = mock_acquire_data

    pipeline_v1 = LoaderPipeline(version=v1_version, output_dir=output_dir, adapter=adapter, mode="FULL")
    pipeline_v1.run()
    LoaderPipeline._acquire_data = original_acquire_data # Restore

    # --- 4. Run DELTA load for v2, but only include molecule_dictionary ---
    pipeline_v2 = LoaderPipeline(
        version=v2_version,
        output_dir=output_dir,
        adapter=adapter,
        mode="DELTA",
        include_tables=["molecule_dictionary"], # The key part of this test
    )
    pipeline_v2.run()

    # --- 5. Assert final state ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            # Assert that molecule_dictionary was updated
            cursor.execute("SELECT pref_name FROM molecule_dictionary WHERE molregno = 1;")
            assert cursor.fetchone()[0] == 'ASPIRIN V2'

            # Assert that target_dictionary was NOT updated
            cursor.execute("SELECT pref_name FROM target_dictionary WHERE tid = 101;")
            assert cursor.fetchone()[0] == 'Cytochrome P450' # Should still be the V1 name

            # Assert metadata log for the delta load
            cursor.execute("""
                SELECT table_name, insert_count, update_count, obsolete_count
                FROM chembl_loader_meta.load_details
                WHERE load_id = %s;
            """, (pipeline_v2.load_id,))
            details = cursor.fetchall()
            assert len(details) == 1 # Should only have a record for molecule_dictionary
            assert details[0][0] == 'molecule_dictionary'
            assert details[0][2] == 1 # 1 update
    finally:
        conn.close()


from py_load_chembl.config import STANDARD_TABLE_SUBSET

@patch("subprocess.run")
def test_full_load_standard_representation_mocked(mock_subprocess_run, postgres_service, tmp_path, requests_mock):
    """
    Tests that a FULL load with the 'standard' representation correctly calls
    pg_restore with the appropriate --table arguments for the standard subset.
    This test mocks the subprocess call to avoid issues with test archive data.
    """
    # --- 1. Setup mocks and test data ---
    chembl_version = "99"
    output_dir = tmp_path / "chembl_data"

    # Create a dummy tar.gz file to satisfy the download and extraction logic
    tar_gz_filename = f"chembl_{chembl_version}_postgresql.tar.gz"
    tar_gz_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_gz_buffer, mode="w:gz") as tar:
        dir_info = tarfile.TarInfo(name=f"chembl_{chembl_version}_postgresql")
        dir_info.type = tarfile.DIRTYPE
        tar.addfile(dir_info)
    tar_gz_content = tar_gz_buffer.getvalue()

    tar_gz_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}/{tar_gz_filename}"
    checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}/checksums.txt"
    checksum = hashlib.md5(tar_gz_content).hexdigest()
    requests_mock.get("https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/", text=f'<a href="chembl_{chembl_version}/"></a>')
    requests_mock.get(tar_gz_url, content=tar_gz_content)
    requests_mock.get(checksum_url, text=f"{checksum}  {tar_gz_filename}")

    # Mock the return value of subprocess.run to prevent it from actually running pg_restore
    mock_subprocess_run.return_value.returncode = 0

    # --- 2. Configure and run the pipeline ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)
    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="FULL",
        output_dir=output_dir,
        include_tables=STANDARD_TABLE_SUBSET, # Use the official standard subset
    )
    pipeline.run()

    # --- 3. Assert that pg_restore was called with the correct arguments ---
    mock_subprocess_run.assert_called_once()
    call_args = mock_subprocess_run.call_args[0][0]

    assert "pg_restore" in call_args
    table_flag_indices = [i for i, arg in enumerate(call_args) if arg == "--table"]
    assert len(table_flag_indices) == len(STANDARD_TABLE_SUBSET)
    for i in table_flag_indices:
        assert call_args[i+1] in STANDARD_TABLE_SUBSET


def test_full_load_with_optimizations(postgres_service, tmp_path, requests_mock, caplog):
    """
    Tests that the full load process correctly uses the pre/post load optimizations.
    Specifically, it should drop pre-existing constraints and indexes before the load.
    """
    # --- 1. Run a normal full load first to get the database into a known state ---
    test_postgres_full_load(postgres_service, tmp_path, requests_mock, caplog)

    # --- 2. Manually add extra objects to the database to simulate a dirty state ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            # Add a dummy, non-standard index
            cursor.execute("CREATE INDEX idx_dummy_test ON public.molecule_dictionary (pref_name);")
            # Add a dummy, non-standard foreign key.
            # First, add a dummy table to reference.
            cursor.execute("CREATE TABLE dummy_ref_table (id integer primary key);")
            cursor.execute("INSERT INTO dummy_ref_table (id) VALUES (1), (2);")
            # Then add a column to molecule_dictionary to create the FK on.
            cursor.execute("ALTER TABLE public.molecule_dictionary ADD COLUMN dummy_ref_id INTEGER;")
            cursor.execute("UPDATE public.molecule_dictionary SET dummy_ref_id = molregno;")
            cursor.execute("""
                ALTER TABLE public.molecule_dictionary
                ADD CONSTRAINT fk_dummy_test
                FOREIGN KEY (dummy_ref_id) REFERENCES public.dummy_ref_table(id);
            """)
        conn.commit()
    finally:
        conn.close()

    # --- 3. Run the FULL load again. The optimization logic should drop the dummy objects. ---
    # We can reuse the mocks from the first run since the URLs are the same.
    chembl_version = "99"
    output_dir = tmp_path / "chembl_data"
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])

    # We need to use the same mock for _acquire_data as the main full load test
    original_acquire_data = LoaderPipeline._acquire_data
    def mock_acquire_data(self):
        self.chembl_version = int(self.version_str)
        self.mode = 'DELTA'
        original_acquire_data(self)
        self.mode = 'FULL'
    LoaderPipeline._acquire_data = mock_acquire_data

    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="FULL",
        output_dir=output_dir,
    )
    pipeline.run()

    LoaderPipeline._acquire_data = original_acquire_data # Restore

    # --- 4. Assert that the dummy objects were dropped ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            # Check that the dummy index is gone
            cursor.execute("""
                SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_dummy_test';
            """)
            assert cursor.fetchone() is None, "Dummy index should have been dropped by the full load."

            # Check that the dummy foreign key is gone
            cursor.execute("""
                SELECT 1 FROM information_schema.table_constraints
                WHERE constraint_schema = 'public' AND constraint_name = 'fk_dummy_test';
            """)
            assert cursor.fetchone() is None, "Dummy foreign key should have been dropped by the full load."

            # Check that the legitimate primary key still exists
            cursor.execute("""
                SELECT 1 FROM information_schema.table_constraints
                WHERE constraint_schema = 'public' AND constraint_name = 'molecule_dictionary_pkey';
            """)
            assert cursor.fetchone() is not None, "Legitimate primary key should exist."

    finally:
        conn.close()
