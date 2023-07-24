from __future__ import annotations

import copy
import json
from collections import OrderedDict
from typing import Any, Dict, List, Literal, Optional, Tuple

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
class RequestParams:
    """
    Parameters for trend

    1. Get tz info from [here](https://forbrains.co.uk/international_tools/earth_timezones).
    """

    tz: int = 120
    hl: str = "en-US"


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
        path_config = convert_path(config["path"])
        path_params = PathParams(**path_config)

        return Config(
            request_params=request_params,
            trend_params=trend_params,
            path_params=path_params,
        )

    def _validate(self):
        assert (
            self.trend_params.keyword == self.path_params.keyword
        ), "trend keyword and path keyword should match"


class ConfigBundle:
    """Build a list of configs from file

    :param file_path: path to the file
    """

    def __init__(self, file_path: AnyPath):
        self.file_path = file_path
        raw_configs = self._load_json(self.file_path)
        self.configs, self.global_config = self._combine_configs(
            raw_configs=raw_configs
        )

    def _combine_configs(self, raw_configs: Dict) -> Tuple[List[Config], Dict]:
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
        global_config = copy.deepcopy(raw_global_config)

        if "path" in global_config:
            global_config["path"] = convert_path(global_config["path"])

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
