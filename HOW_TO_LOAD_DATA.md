# How to Load ChEMBL Data Using This Package

This guide explains how to use the `py-load-chembl` package to download and load ChEMBL data into a database. The package is designed to be a robust and high-performance tool for this purpose, abstracting away the manual steps of downloading, extracting, and loading the data.

## 1. Installation

The package uses a modular design and requires you to install the "extra" for your specific database target. As of now, the package provides a production-ready adapter for PostgreSQL.

You can install the package and the PostgreSQL adapter using `pip` from the root of this project:

```bash
pip install .[postgres]
```

## 2. Configuration

The primary way to configure the database connection is by setting the `CHEMBL_DB_TARGET` environment variable. This variable holds the connection string for your target database.

For a PostgreSQL database, the connection string format is:
`postgresql://<user>:<password>@<host>:<port>/<database>`

Here is an example of how to set the environment variable. Replace the placeholders with your actual database credentials.

```bash
export CHEMBL_DB_TARGET="postgresql://my_user:my_password@localhost:5432/chembl_db"
```

You can also pass the connection string directly to the command line with the `--target` option, but using the environment variable is recommended for keeping secrets out of your shell history.

## 3. Loading Data

The primary function of this package is to perform a "full load" of the ChEMBL database. This process will create the database schema and load all data for a specific ChEMBL release. The script you provided performs a similar full load.

### Full Representation

To load the entire ChEMBL database, run the following command. It will download the latest version of ChEMBL and load it into the database you configured in the previous step.

```bash
py-load-chembl load --mode FULL --version latest
```

This is equivalent to running:
```bash
py-load-chembl load --mode FULL --version latest --representation full
```

### Standard Representation

For many use cases, only a subset of the ChEMBL tables is needed. The package provides a "standard" representation that includes the most commonly used tables (like `chembl_activities`, `molecule_dictionary`, etc.). This is much faster and requires less storage.

To load the standard representation, use the `--representation` flag:

```bash
py-load-chembl load --mode FULL --version latest --representation standard
```

## 4. Note on Databricks Support

The script you provided is a custom solution for loading ChEMBL data specifically into a Databricks environment. It manually handles the download, extraction, schema conversion (from PostgreSQL to Databricks-compatible SQL), and data loading steps.

This package, `py-load-chembl`, automates all of those steps but is currently implemented to target a **PostgreSQL** database.

The core logic of this package is designed to be extensible. To add support for Databricks, a new "Databricks adapter" would need to be created. This adapter would be responsible for handling the specifics of interacting with Databricks, such as:
- Generating Databricks-compatible `CREATE TABLE` statements (e.g., `USING DELTA`).
- Using the most efficient bulk-loading mechanism for Databricks (e.g., `COPY INTO` from a storage location).

While this package does not support Databricks out-of-the-box, the functionality of your script could be migrated into a new Databricks adapter, following the pattern of the existing `PostgresAdapter`.
