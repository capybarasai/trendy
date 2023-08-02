import copy
import datetime
from collections import OrderedDict
from typing import Dict, Literal, Optional, Union

from cloudpathlib import AnyPath
from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from rich.panel import Panel
from rich.table import Table
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

    def s3_access_point(
        self,
        base_url: str,
        snapshot_date: Union[datetime.date, Literal["latest"]] = "latest",
        filename: Optional[str] = "data.json",
    ):
        """
        An access point is some kind of URL. For example
        https://sm-google-trend-public.s3.eu-central-1.amazonaws.com/
        agg/keyword=curtain/cat=0/geo=de/timeframe=today-5-y/format=json/
        snapshot_date=latest/data.json

        :param base_url: base url to append the path to
        :param snapshot_date: snapshot of the file
        :param filename: name of the file
        """
        if base_url.endswith("/"):
            base_url = base_url[:-1]

        url = base_url
        for k in self.path_schema:
            url += f"/{k}={self.path_schema[k]}"

        url += f"/snapshot_date={snapshot_date}/{filename}"

        return url


class ConfigTable:
    """
    ConfigBundle to table

    :config_bundle: serpapi config bundle
    """

    def __init__(self, config_bundle):
        self.config_bundle = config_bundle

    def table(self, top_n: int = 10):
        """
        :param top_n: only display the top_n configs
        """
        parent_folder = self.config_bundle.global_config.get("path", {}).get(
            "parent_folder"
        )
        n_configs = len(self.config_bundle)
        table = Table(
            title=(
                f"Configs: {n_configs};\n"
                f"Path: {str(parent_folder)};\n"
                f"Top N: {top_n}"
            ),
            show_lines=True,
        )

        table.add_column("q", justify="right", style="cyan", no_wrap=False)
        table.add_column("cat", style="magenta", no_wrap=False)
        table.add_column("geo", style="magenta", no_wrap=False)
        table.add_column("timeframe", style="magenta", no_wrap=False)

        for idx, c in enumerate(self.config_bundle):
            if idx > top_n:
                break
            else:
                table.add_row(
                    c.serpapi_params.q,
                    c.serpapi_params.cat,
                    c.serpapi_params.geo,
                    c.serpapi_params.date,
                )

        return table
