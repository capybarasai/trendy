import copy
from collections import OrderedDict
from typing import Dict, Literal, Optional

from cloudpathlib import AnyPath
from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from slugify import slugify


def convert_path(raw_config: Dict) -> Dict:
    """Convert str representation of path to Path

    :param raw_config: raw config
    """
    keys = ["parent_folder"]
    config = copy.deepcopy(raw_config)

    for k in keys:
        if (not isinstance(config.get(k), AnyPath)) and (config.get(k) is not None):
            config[k] = config[k]

    return config


@dataclass
class TrendParams:
    keyword: Optional[str] = None
    geo: Optional[str] = None
    timeframe: Optional[str] = None
    cat: Optional[int] = None


class PathParams(BaseModel):
    """parameters to be used to build the folder
    path of the data files.

    !!! note:
        We will have to keep the order of the parameters.

        Pydantic already tries to provide ordered schema.
        In addition, we added a property called `path_schema`.
    """

    keyword: str
    cat: str
    geo: str
    timeframe: str

    @property
    def path_schema(self) -> OrderedDict:
        """Ordered dictionary for the path schema

        Note that keyword and category will be slugified
        """
        return OrderedDict(
            [
                ("keyword", slugify(self.keyword)),
                ("cat", slugify(self.cat)),
                ("geo", slugify(self.geo)),
                ("timeframe", slugify(self.timeframe)),
            ]
        )

    def path(self, parent_folder: AnyPath) -> AnyPath:
        """build the path under the parent folder

        :parent_folder: base path
        """
        if not isinstance(parent_folder, AnyPath):
            parent_folder = AnyPath(parent_folder)
        folder = parent_folder
        for k in self.path_schema:
            folder = folder / f"{k}={self.path_schema[k]}"

        return folder
