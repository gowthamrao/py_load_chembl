import typer
from pathlib import Path
from typing_extensions import Annotated
import logging
from py_load_chembl.logging_config import setup_logging

from py_load_chembl.adapters.postgres import PostgresAdapter
from py_load_chembl.pipeline import LoaderPipeline

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
):
    """
    Downloads and loads ChEMBL data into a target database.
    """
    setup_logging()
    logger.info("Initiating ChEMBL load", extra={"target": target, "mode": mode, "version": version})

    # For now, we only support postgres
    if not target.startswith("postgresql"):
        logger.critical("Only postgresql targets are currently supported.", extra={"target": target})
        raise typer.Exit(code=1)

    adapter = PostgresAdapter(connection_string=target)
    pipeline = LoaderPipeline(adapter=adapter, version=version, mode=mode, output_dir=output_dir)

    try:
        pipeline.run()
        typer.echo("\n[bold green]ChEMBL load process completed successfully![/bold green]")
    except (ConnectionError, ValueError, RuntimeError) as e:
        logger.critical(f"A critical error occurred during the load process: {e}", exc_info=True)
        raise typer.Exit(code=1)


@app.command()
def download(
    version: VersionOption = "latest",
    output_dir: OutputDirOption = Path("./chembl_data"),
):
    """
    Downloads ChEMBL data files without loading them into a database.
    """
    setup_logging()
    logger.info("Initiating ChEMBL download only", extra={"version": version, "output_dir": str(output_dir)})

    # No adapter or mode is needed for download-only
    pipeline = LoaderPipeline(version=version, output_dir=output_dir)

    try:
        # The pipeline's _acquire_data is a protected member, but for the CLI's purpose,
        # this is a clean way to reuse the data acquisition logic.
        pipeline._acquire_data()
        typer.echo("\n[bold green]ChEMBL download process completed successfully![/bold green]")
    except (ConnectionError, ValueError) as e:
        logger.critical(f"A critical error occurred during the download process: {e}", exc_info=True)
        raise typer.Exit(code=1)


def main():
    app()

if __name__ == "__main__":
    main()
