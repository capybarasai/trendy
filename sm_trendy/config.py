from collections import OrderedDict
from dataclasses import dataclass
from typing import Literal

from cloudpathlib import AnyPath
from pydantic import BaseModel
from slugify import slugify

# class Config:
#     """
#     :param path: path to the config file
#     """
#     def __init__(self, path: AnyPath):
#         self.path = path

#     def __call__(self):


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
        """Ordered dictionary for the path schema"""
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
        folder = parent_folder
        for k in self.path_schema:
            folder = folder / f"{k}={self.path_schema[k]}"

        return folder
