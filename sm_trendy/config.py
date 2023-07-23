from __future__ import annotations

import copy
import json
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from cloudpathlib import AnyPath
from pydantic import BaseModel
from slugify import slugify


@dataclass
class RequestParams:
    """
    Parameters for trend

    1. Get tz info from [here](https://forbrains.co.uk/international_tools/earth_timezones).
    """

    tz: int = 120
    hl: str = "en-US"


@dataclass
class TrendParams:
    keyword: str
    geo: str
    timeframe: str
    cat: int


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
        folder = parent_folder
        for k in self.path_schema:
            folder = folder / f"{k}={self.path_schema[k]}"

        return folder


class Config:
    """
    Config for downloading trends

    :param request_params: params for the TrendReq
    :param trend_params: params for SingleTrend
    :param path_params: params to build the path
    """

    def __init__(
        self,
        request_params: RequestParams,
        trend_params: TrendParams,
        path_params: PathParams,
    ):
        self.request_params = request_params
        self.trend_params = trend_params
        self.path_params = path_params

    @classmethod
    def from_dict(cls, config: Dict) -> Config:
        """
        Load config from a dictionary

        ```python
        config = {
            "request": {
                "tz": 120,
                "hl": "en-US"
            },
            "trend": {
                "timeframe": "today 5-y",
                "cat": 0,
                "geo": "DE",
                "keyword": "phone case"
            },
            "path": {
                "keyword": "phone case",
                "category": "all",
                "country": "DE",
                "frequency": "1W"
            }
        }
        ```

        :param config: config dictionary that contains
            the key and values to build the params
        """

        request_params = RequestParams(**config["request"])
        trend_params = TrendParams(**config["trend"])
        path_params = PathParams(**config["path"])

        return Config(
            request_params=request_params,
            trend_params=trend_params,
            path_params=path_params,
        )


class ConfigBundle:
    """Build a list of configs from"""

    def __init__(self, file_path: AnyPath):
        self.file_path = file_path
        raw_configs = self._load_json(self.file_path)
        self.configs, self.global_config = self._combine_configs(
            raw_configs=raw_configs
        )

    def _combine_configs(self, raw_configs: Dict) -> List[Config]:
        global_config = self._transform_raw_global_config(raw_configs["global"])

        combined_configs = [
            self._combine_with_global(global_config=global_config, keyword_config=k)
            for k in raw_configs["keywords"]
        ]

        return combined_configs, global_config

    @staticmethod
    def _transform_raw_global_config(raw_global_config: Dict) -> Dict:
        """
        Convert string to path object if exist
        """
        parent_folder = raw_global_config.get("path", {}).get("parent_folder")
        global_config = copy.deepcopy(raw_global_config)

        if parent_folder is None:
            return global_config
        else:
            parent_folder = AnyPath(parent_folder)
            global_config["path"]["parent_folder"] = parent_folder
            return global_config

    @staticmethod
    def _combine_with_global(global_config: Dict, keyword_config: Dict) -> Config:
        new_keyword_config = {}

        for key in ["request", "trend", "path"]:
            new_keyword_config[key] = keyword_config.get(key, {}) | global_config.get(
                key, {}
            )

        return Config.from_dict(new_keyword_config)

    @staticmethod
    def _load_json(file_path: AnyPath) -> Dict:

        with open(file_path, "r") as fp:
            data = json.load(fp)

        return data

    def __getitem__(self, idx: int) -> Config:
        return self.configs[idx]

    def __len__(self) -> int:
        return len(self.configs)

    def __iter__(self):
        for item in self.configs:
            yield item
