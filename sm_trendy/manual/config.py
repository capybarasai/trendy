import json
from typing import Any

from cloudpathlib import AnyPath

from sm_trendy.use_serpapi.config import SerpAPIConfigBundle


class SerpAPI2Manual:
    """Convert SerpAPI config to Manual config,
    and saves the results as json files.

    :param manual_folder: intermediate folder to hold the
        downloaded csv and manual config
    """

    def __init__(self, manual_folder: AnyPath):
        if manual_folder.exists():
            assert (
                len(list(manual_folder.iterdir())) == 0
            ), "An empty folder is required"

        self.manual_folder = manual_folder

    def __call__(self, config_bundle: SerpAPIConfigBundle):
        """
        convert `SerpAPIConfigBundle` to a bunch of folders
        with manual config inside.
        """

        for c in config_bundle:
            manual_config = c.path_params.model_dump()
            c_temp_path = c.path_params.path(parent_folder=self.manual_folder)
            c_temp_path.mkdir(parents=True, exist_ok=True)

            with open(c_temp_path / "manual.json", "w") as fp:
                json.dump(manual_config, fp)
