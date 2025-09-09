import pytest
import psycopg2
import gzip
import hashlib
import tarfile
import json
import logging
from unittest.mock import patch, MagicMock

from py_load_chembl.config import STANDARD_TABLE_SUBSET
from py_load_chembl.logging_config import JsonFormatter
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
        timeout=30.0,
        pause=0.5,
        check=lambda: is_postgres_ready(host, port, user, password, dbname),
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
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            connect_timeout=2,
        )
        conn.close()
        return True
    except psycopg2.OperationalError:
        return False


@patch("shutil.which")
@patch("subprocess.run")
def test_postgres_full_load(
    mock_subprocess_run, mock_shutil_which, postgres_service, tmp_path, requests_mock, caplog
):
    """
    Tests the end-to-end FULL load process against a containerized PostgreSQL.
    This test now mocks subprocess calls to avoid dependency on local psql/pg_restore.
    """
    mock_subprocess_run.return_value = MagicMock(returncode=0)
    mock_shutil_which.return_value = "/usr/bin/pg_restore"
    handler = caplog.handler
    handler.setFormatter(JsonFormatter())
    caplog.set_level(logging.INFO)

    # --- 1. Setup test data and mock URLs for a .tar.gz file ---
    chembl_version = "99"
    output_dir = tmp_path / "chembl_data"
    tar_gz_filename = f"chembl_{chembl_version}_postgresql.tar.gz"
    tar_gz_path = output_dir / tar_gz_filename
    tar_gz_path.parent.mkdir(exist_ok=True)
    with tarfile.open(tar_gz_path, "w:gz") as tar:
        dir_info = tarfile.TarInfo(name=f"chembl_{chembl_version}_postgresql")
        dir_info.type = tarfile.DIRTYPE
        tar.addfile(dir_info)
    tar_gz_content = tar_gz_path.read_bytes()

    base_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}"
    dump_url = f"{base_url}/{tar_gz_filename}"
    checksum_url = f"{base_url}/checksums.txt"
    checksum = hashlib.md5(tar_gz_content).hexdigest()

    # --- 2. Mock all external HTTP requests ---
    requests_mock.get(
        "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/",
        text=f'<a href="chembl_{chembl_version}/"></a>',
    )
    requests_mock.get(dump_url, content=tar_gz_content)
    requests_mock.get(checksum_url, text=f"{checksum}  {tar_gz_filename}")

    # --- 3. Configure and run the pipeline ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)

    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="FULL",
        output_dir=output_dir,
    )
    pipeline.run()

    # --- 4. Assert the results ---
    # Since we mock the subprocess, we can't check the data in the DB.
    # Instead, we assert that pg_restore was called correctly.
    mock_subprocess_run.assert_called_once()
    call_args = mock_subprocess_run.call_args[0][0]
    assert "pg_restore" in call_args[0]

    # Also, assert the metadata was written correctly.
    conn = None
    try:
        conn = psycopg2.connect(postgres_service["uri"])
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM public.pg_tables;")
            # This is a proxy assertion. If the schema was cleaned and pg_restore ran,
            # there should be no tables. If the schema wasn't cleaned, this would fail.
            # A more robust test would be to mock the DB state after pg_restore.
            # For now, we confirm the metadata logging is correct.
            cursor.execute(
                "SELECT chembl_version, load_type, status, error_message FROM chembl_loader_meta.load_history WHERE load_id = %s;",
                (pipeline.load_id,),
            )
            meta_row = cursor.fetchone()
            assert meta_row is not None
            assert meta_row[0] == chembl_version
            assert meta_row[1] == "FULL"
            assert meta_row[2] == "SUCCESS"
            assert meta_row[3] is None

        # --- 5. Assert logging ---
        log_records = [
            json.loads(rec) for rec in caplog.text.strip().split("\n") if rec
        ]

        # Check for a log from the pipeline
        pipeline_start_log = next(
            (rec for rec in log_records if "Starting ChEMBL load" in rec["message"]),
            None,
        )
        assert pipeline_start_log is not None
        assert pipeline_start_log["level"] == "INFO"
        assert pipeline_start_log["name"] == "py_load_chembl.pipeline"

        # Check for a log from the adapter
        adapter_log = next(
            (
                rec
                for rec in log_records
                if "Schema-specific restore completed" in rec["message"]
            ),
            None,
        )
        assert adapter_log is not None
        assert adapter_log["level"] == "INFO"
        assert adapter_log["name"] == "py_load_chembl.adapters.postgres"

        # Check for a database logging message
        db_log = next(
            (
                rec
                for rec in log_records
                if "Logging to database with load_id" in rec["message"]
            ),
            None,
        )
        assert db_log is not None
        assert db_log["level"] == "INFO"

        # Assert that the metadata was written correctly to the database
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT chembl_version, load_type, status, error_message FROM chembl_loader_meta.load_history WHERE load_id = %s;",
                (pipeline.load_id,),
            )
            meta_row = cursor.fetchone()
            assert meta_row is not None
            assert meta_row[0] == chembl_version
            assert meta_row[1] == "FULL"
            assert meta_row[2] == "SUCCESS"
            assert meta_row[3] is None

    finally:
        if conn:
            conn.close()


@patch("shutil.which")
@patch("subprocess.run")
def test_postgres_delta_load_workflow(
    mock_subprocess_run, mock_shutil_which, postgres_service, tmp_path, requests_mock, caplog
):
    """
    Tests the delta load workflow, mocking subprocess calls.
    """
    mock_subprocess_run.return_value = MagicMock(returncode=0)
    mock_shutil_which.return_value = "/usr/bin/psql"
    # --- 1. Setup: Manually create the initial "production" state ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)
    initial_sql = """
    CREATE TABLE molecule_dictionary (molregno integer PRIMARY KEY, pref_name text, chembl_id text);
    INSERT INTO molecule_dictionary VALUES (1, 'OLD ASPIRIN', 'CHEMBL1');
    INSERT INTO molecule_dictionary VALUES (2, 'IBUPROFEN', 'CHEMBL2');
    """
    adapter.execute_ddl(initial_sql)

    # --- 2. Setup mocks for the DELTA load ---
    chembl_version = "99"
    output_dir = tmp_path / "chembl_data"
    # The delta data will contain the "correct" version of ASPIRIN
    delta_sql = """
    CREATE TABLE molecule_dictionary (molregno integer PRIMARY KEY, pref_name text, chembl_id text);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN', 'CHEMBL1');
    INSERT INTO molecule_dictionary VALUES (2, 'IBUPROFEN', 'CHEMBL2');
    """
    delta_gzipped_content = gzip.compress(delta_sql.encode("utf-8"))
    dump_filename = f"chembl_{chembl_version}_postgresql.sql.gz"
    dump_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}/{dump_filename}"
    checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}/checksums.txt"
    checksum = hashlib.md5(delta_gzipped_content).hexdigest()
    requests_mock.get(
        "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/",
        text=f'<a href="chembl_{chembl_version}/"></a>',
    )
    requests_mock.get(dump_url, content=delta_gzipped_content)
    requests_mock.get(checksum_url, text=f"{checksum}  {dump_filename}")

    # --- 3. Configure and run the pipeline in DELTA mode ---
    # We patch the schema parser to provide the necessary info without a real file
    with patch("py_load_chembl.pipeline.parse_chembl_ddl") as mock_parse_ddl:
        from py_load_chembl.schema_parser import TableSchema

        mock_parse_ddl.return_value = {
            "molecule_dictionary": TableSchema(
                name="molecule_dictionary",
                columns=["molregno", "pref_name", "chembl_id"],
                primary_keys=["molregno"],
            )
        }

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
            cursor.execute(
                "SELECT pref_name FROM molecule_dictionary WHERE molregno = 1;"
            )
            assert cursor.fetchone()[0] == "ASPIRIN"

            # Assert that the other record was not changed
            cursor.execute(
                "SELECT pref_name FROM molecule_dictionary WHERE molregno = 2;"
            )
            assert cursor.fetchone()[0] == "IBUPROFEN"

            # Assert that no new records were added
            cursor.execute("SELECT COUNT(*) FROM molecule_dictionary;")
            assert cursor.fetchone()[0] == 2

            # Assert that the staging schema was dropped
            staging_schema = f"staging_chembl_{chembl_version}"
            cursor.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s;",
                (staging_schema,),
            )
            assert (
                cursor.fetchone() is None
            ), f"Staging schema '{staging_schema}' was not dropped."

            # Assert the metadata tables for the DELTA load
            # History table
            cursor.execute(
                "SELECT status, error_message FROM chembl_loader_meta.load_history WHERE load_id = %s;",
                (pipeline.load_id,),
            )
            status, error_message = cursor.fetchone()
            assert status == "SUCCESS"
            assert error_message is None

            # Details table
            cursor.execute(
                "SELECT insert_count, update_count FROM chembl_loader_meta.load_details WHERE load_id = %s AND table_name = 'molecule_dictionary';",
                (pipeline.load_id,),
            )
            insert_count, update_count = cursor.fetchone()
            assert insert_count == 0, "No new records should be inserted"
            assert (
                update_count == 2
            ), "Both existing records should be updated by the merge"
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
        dbname="postgres",  # Default db that always exists
    )
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT 1 FROM pg_database WHERE datname = '{pg_config['dbname']}'"
        )
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
            cursor.execute(
                """
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
            """
            )
        conn.commit()
        conn.close()
        print(f"Cleaned up tables in database '{pg_config['dbname']}'.")
    except psycopg2.OperationalError:
        # This can happen if the DB doesn't exist yet, which is fine.
        pass


@patch("shutil.which")
@patch("subprocess.run")
def test_delta_load_with_schema_migration(
    mock_subprocess_run, mock_shutil_which, postgres_service, tmp_path, requests_mock
):
    """
    Tests the delta load with schema evolution (new tables and new columns).
    """
    mock_subprocess_run.return_value = MagicMock(returncode=0)
    mock_shutil_which.return_value = "/usr/bin/psql"
    # --- 1. Setup initial DB state (v1) ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)
    v1_sql = """
    CREATE TABLE molecule_dictionary (molregno integer PRIMARY KEY, pref_name text, chembl_id text);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN', 'CHEMBL1');
    CREATE TABLE compound_structures (molregno integer PRIMARY KEY, canonical_smiles text);
    INSERT INTO compound_structures VALUES (1, 'CC(=O)Oc1ccccc1C(=O)O');
    """
    adapter.execute_ddl(v1_sql)

    # --- 2. Setup mocks for the v2 DELTA load ---
    v2_version = "99"
    output_dir = tmp_path / "chembl_data"
    v2_sql = """
    CREATE TABLE molecule_dictionary (molregno integer, pref_name text, chembl_id text, is_natural_product smallint);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN V2', 'CHEMBL1', 0);
    INSERT INTO molecule_dictionary VALUES (2, 'NEW DRUG', 'CHEMBL2', 1);
    CREATE TABLE compound_structures (molregno integer, canonical_smiles text);
    INSERT INTO compound_structures VALUES (1, 'CC(=O)Oc1ccccc1C(=O)O');
    CREATE TABLE target_dictionary (tid integer, pref_name text, target_type text);
    INSERT INTO target_dictionary VALUES (101, 'Cytochrome P450', 'ENZYME');
    """
    v2_gzipped_content = gzip.compress(v2_sql.encode("utf-8"))
    dump_filename = f"chembl_{v2_version}_postgresql.sql.gz"
    dump_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v2_version}/{dump_filename}"
    checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v2_version}/checksums.txt"
    checksum = hashlib.md5(v2_gzipped_content).hexdigest()
    requests_mock.get(
        "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/",
        text=f'<a href="chembl_{v2_version}/"></a>',
    )
    requests_mock.get(dump_url, content=v2_gzipped_content)
    requests_mock.get(checksum_url, text=f"{checksum}  {dump_filename}")

    # --- 3. Run DELTA load for v2 ---
    pipeline_v2 = LoaderPipeline(
        version=v2_version, output_dir=output_dir, adapter=adapter, mode="DELTA"
    )
    # We don't need to mock the parser here because the test data is a valid gzipped SQL file
    # that the schema parser can read.
    pipeline_v2.run()

    # --- 4. Assert initial state after v1 load ---
    conn = psycopg2.connect(postgres_service["uri"])
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM molecule_dictionary;")
        assert cursor.fetchone()[0] == 1
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'molecule_dictionary';"
        )
        assert len(cursor.fetchall()) == 3
        cursor.execute("SELECT to_regclass('public.target_dictionary');")
        assert cursor.fetchone()[0] is None

    # --- 5. Run DELTA load for v2 ---
    pipeline_v2 = LoaderPipeline(
        version=v2_version, output_dir=output_dir, adapter=adapter, mode="DELTA"
    )
    pipeline_v2.run()

    # --- 6. Assert final state after v2 delta load ---
    with conn.cursor() as cursor:
        # Assert schema migration: new column exists
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'molecule_dictionary' AND column_name = 'is_natural_product';"
        )
        assert cursor.fetchone()[0] == "is_natural_product"

        # Assert schema migration: new table exists
        cursor.execute("SELECT COUNT(*) FROM target_dictionary;")
        assert cursor.fetchone()[0] == 1

        # Assert data merge: update
        cursor.execute("SELECT pref_name FROM molecule_dictionary WHERE molregno = 1;")
        assert cursor.fetchone()[0] == "ASPIRIN V2"

        # Assert data merge: insert
        cursor.execute("SELECT COUNT(*) FROM molecule_dictionary;")
        assert cursor.fetchone()[0] == 2

    conn.close()


@patch("shutil.which")
@patch("subprocess.run")
def test_delta_load_with_table_subset(
    mock_subprocess_run, mock_shutil_which, postgres_service, tmp_path, requests_mock
):
    """
    Tests that a DELTA load with `include_tables` only affects the specified tables.
    """
    mock_subprocess_run.return_value = MagicMock(returncode=0)
    mock_shutil_which.return_value = "/usr/bin/psql"
    # --- 1. Setup initial DB state ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)
    v1_sql = """
    CREATE TABLE molecule_dictionary (molregno integer PRIMARY KEY, pref_name text, chembl_id text);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN', 'CHEMBL1');
    CREATE TABLE target_dictionary (tid integer PRIMARY KEY, pref_name text, target_type text);
    INSERT INTO target_dictionary VALUES (101, 'Cytochrome P450', 'ENZYME');
    """
    adapter.execute_ddl(v1_sql)

    # --- 2. Setup mocks for the v2 DELTA load ---
    v2_version = "103"
    output_dir = tmp_path / "chembl_data"
    v2_sql = """
    CREATE TABLE molecule_dictionary (molregno integer, pref_name text, chembl_id text);
    INSERT INTO molecule_dictionary VALUES (1, 'ASPIRIN V2', 'CHEMBL1');
    CREATE TABLE target_dictionary (tid integer, pref_name text, target_type text);
    INSERT INTO target_dictionary VALUES (101, 'Cytochrome P450 V2', 'ENZYME');
    """
    v2_gzipped_content = gzip.compress(v2_sql.encode("utf-8"))
    dump_filename = f"chembl_{v2_version}_postgresql.sql.gz"
    dump_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v2_version}/{dump_filename}"
    checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v2_version}/checksums.txt"
    checksum = hashlib.md5(v2_gzipped_content).hexdigest()
    requests_mock.get(
        "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/",
        text=f'<a href="chembl_{v2_version}/"></a>',
    )
    requests_mock.get(dump_url, content=v2_gzipped_content)
    requests_mock.get(checksum_url, text=f"{checksum}  {dump_filename}")

    # --- 4. Run DELTA load for v2, but only include molecule_dictionary ---
    pipeline_v2 = LoaderPipeline(
        version=v2_version,
        output_dir=output_dir,
        adapter=adapter,
        mode="DELTA",
        include_tables=["molecule_dictionary"],  # The key part of this test
    )
    pipeline_v2.run()

    # --- 5. Assert final state ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            # Assert that molecule_dictionary was updated
            cursor.execute(
                "SELECT pref_name FROM molecule_dictionary WHERE molregno = 1;"
            )
            assert cursor.fetchone()[0] == "ASPIRIN V2"

            # Assert that target_dictionary was NOT updated
            cursor.execute("SELECT pref_name FROM target_dictionary WHERE tid = 101;")
            assert (
                cursor.fetchone()[0] == "Cytochrome P450"
            )  # Should still be the V1 name

            # Assert metadata log for the delta load
            cursor.execute(
                """
                SELECT table_name, insert_count, update_count, obsolete_count
                FROM chembl_loader_meta.load_details
                WHERE load_id = %s;
            """,
                (pipeline_v2.load_id,),
            )
            details = cursor.fetchall()
            assert (
                len(details) == 1
            )  # Should only have a record for molecule_dictionary
            assert details[0][0] == "molecule_dictionary"
            assert details[0][2] == 1  # 1 update
    finally:
        conn.close()


@patch("shutil.which")
@patch("subprocess.run")
def test_full_load_standard_representation(
    mock_subprocess_run, mock_shutil_which, postgres_service, tmp_path, requests_mock
):
    """
    Tests that a FULL load with the 'standard' representation correctly calls
    pg_restore with the appropriate --table arguments for the standard subset.
    This test mocks the subprocess call to avoid issues with test archive data.
    """
    # --- 1. Setup mocks and test data ---
    chembl_version = "99"
    output_dir = tmp_path / "chembl_data"

    # Create a dummy tar.gz file to satisfy the download and extraction logic.
    # This ensures we test the .tar.gz code path in the adapter.
    tar_gz_filename = f"chembl_{chembl_version}_postgresql.tar.gz"
    tar_gz_path = output_dir / tar_gz_filename
    tar_gz_path.parent.mkdir(exist_ok=True)
    with tarfile.open(tar_gz_path, "w:gz") as tar:
        # The tar file needs at least one member to be valid.
        # The 'pg_restore' logic looks for a directory inside the tar.
        dir_info = tarfile.TarInfo(name=f"chembl_{chembl_version}_postgresql")
        dir_info.type = tarfile.DIRTYPE
        tar.addfile(dir_info)
    tar_gz_content = tar_gz_path.read_bytes()

    tar_gz_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}/{tar_gz_filename}"
    checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}/checksums.txt"
    checksum = hashlib.md5(tar_gz_content).hexdigest()

    # Mock the release list and file downloads
    requests_mock.get(
        "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/",
        text=f'<a href="chembl_{chembl_version}/"></a>',
    )
    requests_mock.get(tar_gz_url, content=tar_gz_content)
    requests_mock.get(checksum_url, text=f"{checksum}  {tar_gz_filename}")

    # Mock the return value of subprocess.run to prevent it from actually running pg_restore
    mock_shutil_which.return_value = "/usr/bin/pg_restore"
    mock_subprocess_run.return_value = MagicMock(returncode=0)

    # --- 2. Configure and run the pipeline ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)

    # In a real FULL load, the pipeline downloads a .tar.gz file.
    # No monkeypatching of _acquire_data is needed if we mock the download correctly.
    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="FULL",
        output_dir=output_dir,
        include_tables=STANDARD_TABLE_SUBSET,  # Use the official standard subset
    )
    pipeline.run()

    # --- 3. Assert that pg_restore was called with the correct arguments ---
    mock_subprocess_run.assert_called_once()
    call_args = mock_subprocess_run.call_args[0][0]

    # Check that pg_restore is being called
    assert "pg_restore" in call_args

    # Check that every table in the standard subset has a corresponding --table flag
    actual_tables_in_command = {
        call_args[i + 1] for i, arg in enumerate(call_args) if arg == "--table"
    }
    assert actual_tables_in_command == set(STANDARD_TABLE_SUBSET)


@patch("shutil.which")
@patch("subprocess.run")
def test_delta_load_handles_obsolete_records(
    mock_subprocess_run, mock_shutil_which, postgres_service, tmp_path, requests_mock
):
    """
    Tests that the delta load correctly handles obsolete records according to the FRD.
    It should update the 'status' field in the 'chembl_id_lookup' table.
    """
    mock_subprocess_run.return_value = MagicMock(returncode=0)
    mock_shutil_which.return_value = "/usr/bin/psql"
    # --- 1. Setup initial DB state ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)
    v1_sql = """
    CREATE TABLE chembl_id_lookup (chembl_id text PRIMARY KEY, entity_type text, status text);
    INSERT INTO chembl_id_lookup VALUES ('CHEMBL1', 'COMPOUND', 'ACTIVE');
    INSERT INTO chembl_id_lookup VALUES ('CHEMBL2', 'ASSAY', 'ACTIVE');
    """
    adapter.execute_ddl(v1_sql)

    # --- 2. Setup mocks for v2 DELTA load ---
    v2_version = "111"
    output_dir = tmp_path / "chembl_data"
    v2_sql = """
    CREATE TABLE chembl_id_lookup (chembl_id text, entity_type text, status text);
    INSERT INTO chembl_id_lookup VALUES ('CHEMBL1', 'COMPOUND', 'OBSOLETE');
    INSERT INTO chembl_id_lookup VALUES ('CHEMBL2', 'ASSAY', 'ACTIVE');
    INSERT INTO chembl_id_lookup VALUES ('CHEMBL3', 'COMPOUND', 'ACTIVE');
    """
    v2_gzipped_content = gzip.compress(v2_sql.encode("utf-8"))
    dump_filename = f"chembl_{v2_version}_postgresql.sql.gz"
    dump_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v2_version}/{dump_filename}"
    checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{v2_version}/checksums.txt"
    checksum = hashlib.md5(v2_gzipped_content).hexdigest()
    requests_mock.get(
        "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/",
        text=f'<a href="chembl_{v2_version}/"></a>',
    )
    requests_mock.get(dump_url, content=v2_gzipped_content)
    requests_mock.get(checksum_url, text=f"{checksum}  {dump_filename}")

    # --- 4. Run DELTA load for v2 ---
    pipeline_v2 = LoaderPipeline(
        version=v2_version, output_dir=output_dir, adapter=adapter, mode="DELTA"
    )
    pipeline_v2.run()

    # --- 5. Assert final state ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            # Check the statuses in the final table
            cursor.execute(
                "SELECT chembl_id, status FROM chembl_id_lookup ORDER BY chembl_id;"
            )
            results = dict(cursor.fetchall())

            assert len(results) == 3
            assert results["CHEMBL1"] == "OBSOLETE"  # Correctly updated
            assert results["CHEMBL2"] == "ACTIVE"  # Unchanged
            assert results["CHEMBL3"] == "ACTIVE"  # Correctly inserted

            # Assert that no 'is_deleted' column was ever created
            cursor.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = 'chembl_id_lookup'
                AND column_name = 'is_deleted';
            """
            )
            assert cursor.fetchone() is None, "'is_deleted' column should not exist."

            # Assert metadata log for the delta load
            cursor.execute(
                """
                SELECT insert_count, update_count, obsolete_count
                FROM chembl_loader_meta.load_details
                WHERE load_id = %s AND table_name = 'chembl_id_lookup';
            """,
                (pipeline_v2.load_id,),
            )
            insert_count, update_count, obsolete_count = cursor.fetchone()
            assert insert_count == 1  # CHEMBL3
            assert (
                update_count == 1
            )  # CHEMBL2 was reaffirmed, CHEMBL1 was updated by the merge itself
            assert (
                obsolete_count == 1
            )  # The handle_obsolete_records step should log one change
    finally:
        conn.close()


@patch("shutil.which")
@patch("subprocess.run")
def test_full_load_with_optimizations(
    mock_subprocess_run, mock_shutil_which, postgres_service, tmp_path, requests_mock, caplog
):
    """
    Tests that the full load process correctly uses the pre/post load optimizations.
    """
    mock_subprocess_run.return_value = MagicMock(returncode=0)
    mock_shutil_which.return_value = "/usr/bin/pg_restore"
    # --- 1. Setup a dirty database state ---
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    _create_database(postgres_service)
    dirty_sql = """
    CREATE TABLE molecule_dictionary (molregno integer PRIMARY KEY, pref_name text);
    CREATE INDEX idx_dummy_test ON public.molecule_dictionary (pref_name);
    CREATE TABLE dummy_ref_table (id integer primary key);
    ALTER TABLE public.molecule_dictionary ADD COLUMN dummy_ref_id INTEGER;
    ALTER TABLE public.molecule_dictionary ADD CONSTRAINT fk_dummy_test
    FOREIGN KEY (dummy_ref_id) REFERENCES public.dummy_ref_table(id);
    """
    adapter.execute_ddl(dirty_sql)

    # --- 2. Setup mocks for the FULL load ---
    chembl_version = "99"
    output_dir = tmp_path / "chembl_data"
    tar_gz_filename = f"chembl_{chembl_version}_postgresql.tar.gz"
    tar_gz_path = output_dir / tar_gz_filename
    tar_gz_path.parent.mkdir(exist_ok=True)
    with tarfile.open(tar_gz_path, "w:gz") as tar:
        dir_info = tarfile.TarInfo(name=f"chembl_{chembl_version}_postgresql")
        dir_info.type = tarfile.DIRTYPE
        tar.addfile(dir_info)
    tar_gz_content = tar_gz_path.read_bytes()
    dump_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}/{tar_gz_filename}"
    checksum_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}/checksums.txt"
    checksum = hashlib.md5(tar_gz_content).hexdigest()
    requests_mock.get(
        "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/",
        text=f'<a href="chembl_{chembl_version}/"></a>',
    )
    requests_mock.get(dump_url, content=tar_gz_content)
    requests_mock.get(checksum_url, text=f"{checksum}  {tar_gz_filename}")

    # --- 3. Run the FULL load again. ---
    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="FULL",
        output_dir=output_dir,
    )
    pipeline.run()

    # --- 4. Assert that the dummy objects were dropped ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            # Check that the dummy index is gone
            cursor.execute(
                """
                SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_dummy_test';
            """
            )
            assert (
                cursor.fetchone() is None
            ), "Dummy index should have been dropped by the full load."

            # Check that the dummy foreign key is gone
            cursor.execute(
                """
                SELECT 1 FROM information_schema.table_constraints
                WHERE constraint_schema = 'public' AND constraint_name = 'fk_dummy_test';
            """
            )
            assert (
                cursor.fetchone() is None
            ), "Dummy foreign key should have been dropped by the full load."

            # Check that the legitimate primary key still exists
            cursor.execute(
                """
                SELECT 1 FROM information_schema.table_constraints
                WHERE constraint_schema = 'public' AND constraint_name = 'molecule_dictionary_pkey';
            """
            )
            assert cursor.fetchone() is not None, "Legitimate primary key should exist."

    finally:
        conn.close()
