import importlib.metadata

try:
    __version__ = importlib.metadata.version("py-load-chembl")
except importlib.metadata.PackageNotFoundError:
    # This happens when the package is not installed in editable mode.
    __version__ = "0.0.0-dev"
