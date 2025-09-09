# py-load-chembl

A Python package for robust, high-performance, and extensible loading of the ChEMBL database into various relational database systems.

## Overview

The `py-load-chembl` package is designed to address the data engineering challenges associated with loading the large (multi-gigabyte, ~80 tables) ChEMBL database. It prioritizes using native bulk loading mechanisms for high performance and implements an efficient "calculated delta" strategy for incremental updates.

This allows organizations to rapidly integrate new ChEMBL releases into their analytical platforms, accelerating research and reducing operational overhead.

### Key Features

- **High-Performance Loading**: Utilizes native database tools (`pg_restore`, `psql`, `COPY`) for maximum performance.
- **Full & Delta Loads**: Supports both full, from-scratch database loads and efficient delta loads to update existing databases with new ChEMBL releases.
- **Dynamic Schema Handling**: Automatically parses ChEMBL's provided DDL to handle schema definitions and primary keys, making it robust against future changes.
- **Schema Migration**: Automatically applies additive schema changes (new tables, new columns) during delta loads.
- **Extensible Architecture**: Built on an adapter pattern, allowing for future extension to other database systems like Redshift, BigQuery, or Databricks.
- **Operational Logging**: Emits structured (JSON) logs for easy integration with log aggregation and monitoring systems.

## Installation

The package can be installed using `pip`. The core package provides the main pipeline logic. You must install the "extras" for your specific database target.

```bash
# Install the core package and the postgres adapter from the project root
pip install .[postgres]
```

## Usage

The primary interface is the Command Line Interface (CLI).

### Configuration

Database connection details are provided via a connection string. This can be passed as an option on the command line or, preferably, set as an environment variable.

```bash
export CHEMBL_DB_TARGET="postgresql://user:password@hostname:5432/chembl_db"
```

Logging verbosity can be controlled with the `LOG_LEVEL` environment variable (e.g., `DEBUG`, `INFO`, `WARNING`). The default is `INFO`.

```bash
# Set log level to DEBUG for more detailed output
export LOG_LEVEL="DEBUG"
```

### Full Load

A **Full Load** will completely wipe the target schema and load a specific ChEMBL version from scratch. This is for initializing a new database.

By default, a full load will import all ~80 tables from the ChEMBL dump. This is the `full` representation.

```bash
# This is equivalent to specifying --representation full
py-load-chembl load \
    --target "$CHEMBL_DB_TARGET" \
    --mode FULL \
    --version 33
```

#### Standard Representation

For many common use cases, only a subset of tables is needed. You can specify the `standard` representation to load only the most essential tables (e.g., `molecule_dictionary`, `activities`, `assays`, `targets`). This is significantly faster and requires less disk space.

```bash
py-load-chembl load \
    --target "$CHEMBL_DB_TARGET" \
    --mode FULL \
    --version 33 \
    --representation standard
```

To load the latest version, use `latest`:

```bash
py-load-chembl load --mode FULL --version latest --representation standard
```

### Delta Load

A **Delta Load** will update an existing database with a newer ChEMBL release. It calculates the difference between the new release and the existing data, then efficiently merges the changes (inserts and updates).

```bash
py-load-chembl load \
    --target "$CHEMBL_DB_TARGET" \
    --mode DELTA \
    --version 34
```

### Download Only

You can also use the tool to download and verify the ChEMBL source files without loading them into a database.

```bash
py-load-chembl download --version 33 --output-dir ./chembl_files
```

## Architecture

The system is built on a modular pipeline and a `DatabaseAdapter` pattern. The core logic for acquiring and processing ChEMBL data is separate from the database-specific implementation. This allows for new database targets to be added by creating a new adapter that implements the required interface for native bulk loading and merging.
