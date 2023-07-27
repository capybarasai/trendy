from __future__ import annotations

import copy
import json
from typing import Dict, List, Literal, Optional, Tuple

from cloudpathlib import AnyPath
from loguru import logger
from pydantic import BaseModel, FieldValidationInfo, field_validator

from sm_trendy.utilities.config import PathParams, convert_path


class SerpAPIParams(BaseModel):
    """

    SerpAPI docs:
    https://serpapi.com/google-trends-api

    ```python
    sc = SerpAPIParams(
        **{
            "api_key": "",
            "q": "Coffee",
            "geo": "DE",
            "data_type": "TIMESERIES",
            "tz": "120",
        }
    )

    # Convert the config to python dictionary
    sc.dict(exclude_none=True)
    ```
    """

    api_key: str
    engine: str = "google_trends"
    q: str
    geo: Optional[str] = None
    data_type: Literal[
        "TIMESERIES", "GEO_MAP", "GEO_MAP_0", "RELATED_TOPICS", "RELATED_QUERIES"
    ] = "TIMESERIES"
    tz: Optional[str] = "120"
    cat: Optional[Literal["0"]] = None
    date: Literal[
        "now 1-H",
        "now 4-H",
        "now 1-d",
        "now 7-d",
        "today 1-m",
        "today 3-m",
        "today 12-m",
        "today 5-y",
        "all",
    ] = "today 5-y"

    @field_validator("date")
    @classmethod
    def date_match_allowed(cls, v: str, info: FieldValidationInfo):
        allowed = [
            "now 1-H",
            "now 4-H",
            "now 1-d",
            "now 7-d",
            "today 1-m",
            "today 3-m",
            "today 12-m",
            "today 5-y",
            "all",
        ]
        if v not in allowed:
            raise ValueError(f"date must be one of {allowed}")

        return v


class SerpAPIConfig:
    """
    Config for downloading trends

    :param trend_params: params for SingleTrend
    :param path_params: params to build the path
    """

    def __init__(
        self,
        serpapi_params: SerpAPIParams,
        path_params: PathParams,
    ):
        self.serpapi_params = serpapi_params
        self.path_params = path_params

    @classmethod
    def from_dict(cls, config: Dict) -> SerpAPIConfig:
        """
        Load config from a dictionary

        ```python
        raw_config = {
            "serpapi": {
                "date": "today 5-y",
                "cat": "0",
                "geo": "DE",
                "q": "phone case",
                "tz": "120"
            }
        }
        ```

        :param config: config dictionary that contains
            the key and values to build the params
        """

        serpapi_params = SerpAPIParams(**config["serpapi"])
        path_params = PathParams(
            **{
                "keyword": serpapi_params.q,
                "cat": serpapi_params.cat,
                "geo": serpapi_params.geo,
                "timeframe": serpapi_params.date,
            }
        )

        return SerpAPIConfig(
            serpapi_params=serpapi_params,
            path_params=path_params,
        )

    def _validate(self):
        assert (
            self.serpapi_params.q == self.path_params.keyword
        ), "trend keyword and path keyword should match"

        assert (
            self.serpapi_params.geo == self.path_params.geo
        ), "trend geo and path geo should match"

        assert hasattr(self.serpapi_params, "q"), "serpapi_params should have q"
        assert hasattr(self.serpapi_params, "geo"), "serpapi_params should have geo"


class SerpAPIConfigBundle:
    """Build a list of configs from file

    :param file_path: path to the file
    """

    def __init__(self, file_path: AnyPath, serpapi_key: Optional[str] = None):
        self.file_path = file_path
        self.serpapi_key = serpapi_key
        self.raw_configs = self._load_json(self.file_path)

    @property
    def configs(self) -> List[SerpAPIConfig]:

        return self._combine_configs(raw_configs=self.raw_configs)

    @property
    def global_config(self) -> Dict:
        return self._transform_raw_global_config(self.raw_configs["global"])

    def _combine_configs(self, raw_configs: Dict) -> Tuple[List[SerpAPIConfig], Dict]:
        global_config = self._transform_raw_global_config(raw_configs["global"])

        combined_configs = [
            self._combine_with_global(
                global_config=global_config,
                keyword_config=k,
                serpapi_key=self.serpapi_key,
            )
            for k in raw_configs["keywords"]
        ]

        return combined_configs

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
    def _combine_with_global(
        global_config: Dict, keyword_config: Dict, serpapi_key: Optional[str] = None
    ) -> SerpAPIConfig:
        new_keyword_config = {}

        for key in ["serpapi"]:
            new_keyword_config[key] = keyword_config.get(key, {}) | global_config.get(
                key, {}
            )
            if serpapi_key is not None:
                new_keyword_config[key]["api_key"] = serpapi_key

        return SerpAPIConfig.from_dict(new_keyword_config)

    @staticmethod
    def _load_json(file_path: AnyPath) -> Dict:

        with open(file_path, "r") as fp:
            data = json.load(fp)

        return data

    def __getitem__(self, idx: int) -> SerpAPIConfig:
        return self.configs[idx]

    def __len__(self) -> int:
        return len(self.configs)

    def __iter__(self):
        for item in self.configs:
            yield item

    def __add__(self, o: List[Dict]):
        keywords = self.raw_configs["keywords"]
        keywords.extend(o)
        self.raw_configs["keywords"] = keywords

    def save_json(self, target_file_path: AnyPath):
        """save raw config to path

        :param target_file_path: where to save the json file
        """
        logger.info(f"Saving to path {target_file_path}")
        with open(target_file_path, "w") as fp:
            json.dump(self.raw_configs, fp)
