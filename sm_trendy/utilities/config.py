from collections import OrderedDict
from typing import Literal, Optional

from cloudpathlib import AnyPath
from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from slugify import slugify


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
    category: Literal["all"]
    country: str
    frequency: Literal["1W"]

    @property
    def path_schema(self) -> OrderedDict:
        """Ordered dictionary for the path schema

        Note that keyword and category will be slugified
        """
        return OrderedDict(
            [
                ("keyword", slugify(self.keyword)),
                ("category", slugify(self.category)),
                ("country", self.country),
                ("frequency", self.frequency),
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
