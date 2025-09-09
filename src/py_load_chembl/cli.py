import typer
from pathlib import Path
from typing import Optional
from typing_extensions import Annotated
import logging
from py_load_chembl.logging_config import setup_logging
from py_load_chembl import api, downloader
from py_load_chembl.config import STANDARD_PROFILE_TABLES

app = typer.Typer(rich_markup_mode="rich")
logger = logging.getLogger(__name__)

# Reusable options for commands
VersionOption = Annotated[
    str,
    typer.Option(
        "--version",
        "-v",
        help="ChEMBL version to load (e.g., '33' or 'latest')",
    ),
]

OutputDirOption = Annotated[
    Path,
    typer.Option(
        "--output-dir",
        "-o",
        help="Directory to store downloaded ChEMBL files",
        rich_help_panel="Advanced Options"
    ),
]

@app.command()
def load(
    target: Annotated[
        str,
        typer.Option(
            "--target",
            "-t",
            help="Database connection string (e.g., postgresql://user:pass@host/db)",
            envvar="CHEMBL_DB_TARGET",
        ),
    ],
    mode: Annotated[
        str,
        typer.Option(
            "--mode",
            "-m",
            help="Loading mode: FULL or DELTA",
        ),
    ] = "FULL",
    version: VersionOption = "latest",
    output_dir: OutputDirOption = Path("./chembl_data"),
    include_tables: Annotated[
        Optional[str],
        typer.Option(
            "--include-tables",
            help="Comma-separated list of tables to load (e.g., 'molecule_dictionary,compound_structures'). Mutually exclusive with --profile.",
        ),
    ] = None,
    profile: Annotated[
        Optional[str],
        typer.Option(
            "--profile",
            help="Use a predefined profile of tables to load. Currently only 'standard' is available. Mutually exclusive with --include-tables.",
        ),
    ] = None,
):
    """
    Downloads and loads ChEMBL data into a target database.
    """
    table_list = None
    if profile and include_tables:
        logger.critical("--profile and --include-tables are mutually exclusive.")
        raise typer.Exit(code=1)

    if profile:
        if profile.lower() == "standard":
            table_list = STANDARD_PROFILE_TABLES
            logger.info(f"Using 'standard' profile, which includes {len(table_list)} tables.")
        else:
            logger.critical(f"Invalid profile '{profile}'. Only 'standard' is available.")
            raise typer.Exit(code=1)
    elif include_tables:
        table_list = include_tables.split(",")

    try:
        if mode.upper() == 'FULL':
            api.full_load(
                connection_string=target,
                chembl_version=version,
                output_dir=output_dir,
                include_tables=table_list,
            )
        elif mode.upper() == 'DELTA':
            api.delta_load(
                connection_string=target,
                chembl_version=version,
                output_dir=output_dir,
                include_tables=table_list,
            )
        else:
            logger.critical(f"Invalid mode: {mode}. Must be 'FULL' or 'DELTA'.")
            raise typer.Exit(code=1)

        typer.echo(f"\n[bold green]ChEMBL {mode.upper()} load process completed successfully![/bold green]")

    except (ConnectionError, ValueError, RuntimeError) as e:
        # The API functions will log the detailed error, so we just need to show the user a clean message.
        logger.critical(f"A critical error occurred during the load process: {e}")
        raise typer.Exit(code=1)


@app.command()
def download(
    version: VersionOption = "latest",
    output_dir: OutputDirOption = Path("./chembl_data"),
):
    """
    Downloads and verifies ChEMBL data files without loading them into a database.
    """
    setup_logging()
    logger.info("Initiating ChEMBL download only", extra={"version": version, "output_dir": str(output_dir)})

    try:
        # This is a CLI-specific convenience. It directly uses the downloader module.
        if version.lower() == "latest":
            chembl_version = downloader.get_latest_chembl_version()
        else:
            chembl_version = int(version)

        # We download the .tar.gz by default as it's the most common use case (full load)
        dump_url, checksum_url = downloader.get_chembl_file_urls(chembl_version, plain_sql=False)

        downloaded_file = downloader.download_file(dump_url, output_dir)
        is_valid = downloader.verify_checksum(downloaded_file, checksum_url)
        if not is_valid:
            raise ValueError("Downloaded file failed checksum verification.")

        typer.echo(f"\n[bold green]ChEMBL {version} download process completed successfully![/bold green]")
        typer.echo(f"File saved to: {downloaded_file}")

    except (ConnectionError, ValueError) as e:
        logger.critical(f"A critical error occurred during the download process: {e}", exc_info=True)
        raise typer.Exit(code=1)


def main():
    app()

if __name__ == "__main__":
    main()
