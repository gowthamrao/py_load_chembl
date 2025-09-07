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


def test_postgres_delta_load(postgres_service, tmp_path, requests_mock):
    """
    Tests the end-to-end DELTA load process for a single table.
    """
    # --- 1. Setup: Create initial state in the database ---
    _create_database(postgres_service)
    conn = psycopg2.connect(postgres_service["uri"])
    with conn.cursor() as cursor:
        # Create schema and table
        cursor.execute("CREATE TABLE molecule_dictionary (molregno integer NOT NULL, chembl_id varchar(20) NOT NULL, pref_name varchar(255), molecule_type varchar(30));")
        cursor.execute("ALTER TABLE molecule_dictionary ADD PRIMARY KEY (molregno);")
        # Insert initial data: one record to be updated, one to be left alone
        cursor.execute("INSERT INTO molecule_dictionary VALUES (1, 'CHEMBL1', 'OLD_NAME', 'Small Molecule');")
        cursor.execute("INSERT INTO molecule_dictionary VALUES (99, 'CHEMBL99', 'UNCHANGED', 'Peptide');")
    conn.commit()
    conn.close()

    # --- 2. Setup: Create test data file and archive for the delta ---
    test_data_dir = tmp_path / "delta_data"
    test_data_dir.mkdir()

    # This file contains one record to UPDATE (molregno=1) and one to INSERT (molregno=3)
    delta_file_content = "1\tCHEMBL1\tNEW_NAME\tSmall Molecule\n3\tCHEMBL3\tNEW_DRUG\tOligonucleotide\n"
    delta_file = test_data_dir / "molecule_dictionary.txt"
    delta_file.write_text(delta_file_content)

    # Create a tar.gz archive that mimics the real ChEMBL structure
    archive_path = tmp_path / "chembl_delta_test.tar.gz"
    chembl_version = "100"
    internal_dir = f"chembl_{chembl_version}_postgresql/chembl_{chembl_version}.txt/"

    import tarfile
    with tarfile.open(archive_path, "w:gz") as tar:
        # We need to add a directory structure inside the tarball
        tar_info = tarfile.TarInfo(name=internal_dir)
        tar_info.type = tarfile.DIRTYPE
        tar.addfile(tarinfo=tar_info)
        tar.add(delta_file, arcname=f"{internal_dir}{delta_file.name}")

    # --- 3. Mock all external HTTP requests ---
    base_url = f"https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_{chembl_version}"
    dump_url = f"{base_url}/chembl_{chembl_version}_postgresql.tar.gz"
    checksum_url = f"{base_url}/checksums.txt"
    requests_mock.get("https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/", text=f'<a href="chembl_{chembl_version}/"></a>')
    requests_mock.get(checksum_url, text=f"md5checksum  {archive_path.name}") # Dummy checksum
    requests_mock.get(dump_url, content=archive_path.read_bytes())

    # --- 4. Configure and run the pipeline in DELTA mode ---
    output_dir = tmp_path / "chembl_data"
    adapter = PostgresAdapter(connection_string=postgres_service["uri"])
    pipeline = LoaderPipeline(
        adapter=adapter,
        version=chembl_version,
        mode="DELTA",
        output_dir=output_dir,
    )
    pipeline.run()

    # --- 5. Assert the final state of the database ---
    conn = psycopg2.connect(postgres_service["uri"])
    try:
        with conn.cursor() as cursor:
            # Check data integrity
            cursor.execute("SELECT COUNT(*) FROM molecule_dictionary;")
            assert cursor.fetchone()[0] == 3 # 2 initial + 1 insert

            cursor.execute("SELECT pref_name FROM molecule_dictionary WHERE molregno = 1;")
            assert cursor.fetchone()[0] == "NEW_NAME" # Updated

            cursor.execute("SELECT pref_name FROM molecule_dictionary WHERE molregno = 99;")
            assert cursor.fetchone()[0] == "UNCHANGED" # Untouched

            cursor.execute("SELECT pref_name FROM molecule_dictionary WHERE molregno = 3;")
            assert cursor.fetchone()[0] == "NEW_DRUG" # Inserted

            # Check metadata
            cursor.execute("SELECT load_type, status FROM chembl_loader_meta.load_history WHERE load_id = %s;", (pipeline.load_id,))
            load_type, status = cursor.fetchone()
            assert load_type == "DELTA"
            assert status == "SUCCESS"

            cursor.execute("SELECT table_name, insert_count, update_count FROM chembl_loader_meta.load_details WHERE load_id = %s;", (pipeline.load_id,))
            table_name, insert_count, update_count = cursor.fetchone()
            assert table_name == "molecule_dictionary"
            assert insert_count == 1
            assert update_count == 1
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
