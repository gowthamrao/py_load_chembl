from py_load_chembl.adapters.base import DatabaseAdapter

class LoaderPipeline:
    """
    The main pipeline for loading ChEMBL data.
    """

    def __init__(self, adapter: DatabaseAdapter, version: str, mode: str):
        self.adapter = adapter
        self.version = version
        self.mode = mode

    def run(self):
        """
        Executes the loading pipeline.
        """
        print(f"Running ChEMBL load for version {self.version} in {self.mode} mode.")
        pass
