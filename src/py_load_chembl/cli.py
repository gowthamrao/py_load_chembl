import typer
from pathlib import Path
from typing_extensions import Annotated

from py_load_chembl.adapters.postgres import PostgresAdapter
from py_load_chembl.pipeline import LoaderPipeline

app = typer.Typer(rich_markup_mode="rich")

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
    version: Annotated[
        str,
        typer.Option(
            "--version",
            "-v",
            help="ChEMBL version to load (e.g., '33' or 'latest')",
        ),
    ] = "latest",
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Directory to store downloaded ChEMBL files",
        ),
    ] = Path("./chembl_data"),
):
    """
    Downloads and loads ChEMBL data into a target database.
    """
    typer.echo(f"Target: {target}")
    typer.echo(f"Mode: {mode}")
    typer.echo(f"Version: {version}")
    typer.echo(f"Output Directory: {output_dir}")

    # For now, we only support postgres
    if not target.startswith("postgresql"):
        typer.echo(f"Error: Only postgresql targets are currently supported.", err=True)
        raise typer.Exit(code=1)

    adapter = PostgresAdapter(connection_string=target)
    pipeline = LoaderPipeline(adapter=adapter, version=version, mode=mode, output_dir=output_dir)

    try:
        pipeline.run()
        typer.echo("\n[bold green]ChEMBL load process completed successfully![/bold green]")
    except (ConnectionError, ValueError) as e:
        typer.echo(f"\n[bold red]An error occurred:[/bold red] {e}", err=True)
        raise typer.Exit(code=1)


def main():
    app()

if __name__ == "__main__":
    main()
