from collections import OrderedDict
from dataclasses import dataclass

from cloudpathlib import AnyPath
from slugify import slugify

# class Config:
#     """
#     :param path: path to the config file
#     """
#     def __init__(self, path: AnyPath):
#         self.path = path

#     def __call__(self):


@dataclass
class PathParams:
    keyword: str
    category: str
    country: str
    frequency: str

    @property
    def __call__(self) -> OrderedDict:
        return OrderedDict(
            [
                ("keyword", slugify(self.keyword)),
                ("category", slugify(self.category)),
                ("country", self.country),
                ("frequency", self.frequency),
            ]
        )


class BuildPath:
    """
    Build path from a base path

    :param parent_folder: the base folder for the path
    """

    def __init__(self, parent_folder):
        self.parent_folder = parent_folder

    def __call__(self, path_params: PathParams) -> AnyPath:
        """build the path

        ```python
        path_params = OrderedDict(
            keyword="phone_case",
            category="all",
            country="DE",
            frequency="1W"
        )
        ```

        :path_params: ordered path parameters
        """
        folder = self.parent_folder
        for k in path_params.order:
            folder = folder / f"{k}={path_params['k']}"

        return folder
