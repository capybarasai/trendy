import datetime
import re
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd
from cloudpathlib import AnyPath
from loguru import logger

from sm_trendy.use_serpapi.config import SerpAPIConfigBundle
from sm_trendy.utilities.config import PathParams
from sm_trendy.utilities.storage import StoreJSON


class DownloadedLoader:
    """
    Load downloaded data as parquet

    !!! warning
        Currently, this class only downloads the latest snapshot.

    !!! todo:
        Allow downloading any snapshot(s)

    :param parent_folder: parent folder of the downloaded dataset
    :param from_format: which format to load the data from
    """

    def __init__(
        self,
        parent_folder: AnyPath,
        from_format: Optional[Literal["csv", "parquet"]] = "csv",
    ):
        self.from_format = from_format
        self.parent_folder = parent_folder

    def _data_path(self, path_params: PathParams) -> AnyPath:
        """
        build the full dataset path

        :param path_params: PathParams to calculate the path patterns
        """
        data_folder = path_params.path(parent_folder=self.parent_folder)

        if self.from_format == "csv":
            format_path = data_folder / f"format={self.from_format}"
            latest_snapshot = self._latest_snapshots(format_path)
            path = (
                format_path
                / f"snapshot_date={latest_snapshot}"
                / f"data.{self.from_format}"
            )
        else:
            raise Exception(f"Not yet supported: reading from {self.from_format}")

        return path

    def _load_as_dataframe(self, data_path: AnyPath) -> pd.DataFrame:
        """
        Load the data file as pandas dataframe

        :param data_path: path to the data file
        """
        if self.from_format == "csv":
            df = pd.read_csv(data_path)
        else:
            raise Exception(f"Not yet supported: reading from {self.from_format}")

        return df

    def __call__(self, path_params: PathParams) -> pd.DataFrame:
        """
        load the data specified in a PathParams as pandas dataframe

        :param path_params: PathParams to calculate the path patterns
        """
        data_path = self._data_path(path_params=path_params)
        df = self._load_as_dataframe(data_path)

        return df

    @staticmethod
    def _latest_snapshots(path: AnyPath):
        path_subfolders = list(path.iterdir())
        logger.debug(f"subfolders: {path_subfolders} in {path}")

        re_sd = re.compile(r"snapshot_date=(\d{4}-\d{2}-\d{2})")

        snapshot_dates = sum(
            [re_sd.findall(i.name) for i in path_subfolders], []
        )  # type: List[str]
        logger.debug(f"snapshot_dates: {snapshot_dates}")

        snapshot_dates_latest = sorted(
            snapshot_dates, key=lambda x: datetime.date.fromisoformat(x)
        )[-1]

        return snapshot_dates_latest


class AggAPIJSON:
    """
    Generate clean and API usable json data

    AggAPIJSON takes in a dataframe as raw data,
    cleans it up, and convert it to dictionary
    that is suitable for JSON format.

    :param fields: a dictionary that maps the original
        columns to the desired keys in the result
    """

    def __init__(self, fields: Optional[Dict[str, str]] = None):
        if fields is None:
            fields = {
                "query": "query",
                "extracted_value": "value",
                "date": "date",
            }
        self.fields = fields
        self.keep_columns = list(fields.keys())

    def __call__(self, dataframe: pd.DataFrame, sort_by: Optional[str] = None) -> Dict:
        df = dataframe.copy()
        df = df[self.keep_columns]
        df.rename(columns=self.fields, inplace=True)
        records = df.to_dict(orient="records")

        if sort_by is not None:
            records = sorted(records, key=lambda x: x[sort_by])

        return records


class AggSerpAPIBundle:
    """
    Aggregate all data from a whole serpapi config file

    In this class, we follow the following flow

    1. Recreate the SerpAPIConfigBundle
    2. Loop through each config in SerpAPIConfigBundle
        a. retrieve the data from each of the config
        b. convert the selected data to json
        c. save it to a new folder with the same path pattern.
    """

    def __init__(self, parent_path: AnyPath):
        self.parent_path = parent_path

    def __call__(self, serpapi_config_path: AnyPath):
        # ReCreate the bundle config for SerpAPI
        scb = SerpAPIConfigBundle(file_path=serpapi_config_path, serpapi_key="")
        scb_parent_folder = scb.global_config["path"]["parent_folder"]

        # Instantiate the data loader
        dll_k = DownloadedLoader(parent_folder=scb_parent_folder, from_format="csv")

        # Loop through the serpapi configs
        logger.info(f"  Looping through {len(scb)} configs")
        for c in scb:
            logger.debug(f"  Aggregating {c}")
            # Path of the raw downloaded data
            c_path = c.path_params.path(parent_folder=scb_parent_folder)
            # Raw dataframe
            c_df = dll_k(c.path_params)

            # aggregate
            c_agg_json = AggAPIJSON()
            c_records = c_agg_json(dataframe=c_df, sort_by="date")

            # save snapshot
            c_snapshot_date = dll_k._latest_snapshots(c_path / "format=csv")
            c_k_target_path = c.path_params.path(parent_folder=self.parent_path)
            logger.debug(
                f"Saving data to {c_k_target_path} with snapshot {c_snapshot_date}..."
            )
            self._store_json(
                snapshot_date=datetime.date.fromisoformat(c_snapshot_date),
                target_folder=c_k_target_path,
                records=c_records,
            )

            # save a copy as latest
            logger.debug(f"Saving data to {c_k_target_path} with snapshot latest ...")
            self._store_json(
                snapshot_date="latest", target_folder=c_k_target_path, records=c_records
            )

    def _store_json(
        self,
        snapshot_date: Union[datetime.date, Literal["latest"]],
        target_folder: AnyPath,
        records: List[Dict],
    ) -> None:
        """
        Save the `records` as json files inside the folder `target_folder`

        :param snapshot_date: a specific snapshot date to use used as a folder name
        :param target_folder: where to save the data
        :param records: data records to be saved
        """
        store_json = StoreJSON(
            target_folder=target_folder,
            snapshot_date=snapshot_date,
        )
        store_json.save(records=records, formats="json")
