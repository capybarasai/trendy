import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd
from cloudpathlib import AnyPath, CloudPath, S3Path
from loguru import logger


class StoreDataFrame:
    """Save dataframe

    We follow the schema

    ```python
    target_folder / "format=parquet" / "snapshot_date="
    ```

    `target_folder` should reflect what data is inside.
    For example, we may use

    ```
    target_folder = some_folder / "google_trend"
    / "keyword=phone_case" / "cat=0"
    / "geo=de" / "timeframe=today-5-y"
    ```

    :param target_folder: parent folder for the data.
        Note that subfolders will be created inside it.
    :param snapshot_date: the date when the data was produced.
        Please stick to UTC date.
    """

    def __init__(self, target_folder: AnyPath, snapshot_date: datetime.date):
        self.target_folder = target_folder

        if not isinstance(snapshot_date, datetime.date):
            raise TypeError(
                f"snapshot_date provided is not date time: {type(snapshot_date)}"
            )
        else:
            self.snapshot_date = snapshot_date

    def save(
        self,
        trend_data: Any,
        formats: Optional[List[Literal["parquet", "csv"]]],
    ):
        """
        Save the trend results

        :param trend_data: the object containing the dataframe and metadata
        :param formats: which formats to save as
        """
        df = trend_data.dataframe
        metadata = trend_data.metadata

        format_dispatcher = {
            "parquet": {
                "method": self._save_parquet,
                "path": self._file_path(format="parquet"),
            },
            "csv": {
                "method": self._save_csv,
                "path": self._file_path(format="csv"),
            },
        }

        for f in formats:
            try:
                f_method = format_dispatcher[f]["method"]
                f_path_data = format_dispatcher[f]["path"]["data"]  # type: ignore
                f_path_metadata = format_dispatcher[f]["path"]["metadata"]  # type: ignore

                f_method(dataframe=df, target_path=f_path_data)  # type: ignore
                self._save_metadata(metadata=metadata, target_path=f_path_metadata)
            except Exception as e:
                logger.error(f"can not save format {f}: {e}")

        df = trend_data.dataframe

    def _file_path(self, format: Literal["parquet", "csv"]) -> Dict[str, AnyPath]:
        """Compute the full path for the target file
        based on the format

        :param format: the file format to be used
        """
        folder = (
            self.target_folder
            / f"format={format}"
            / f"snapshot_date={self.snapshot_date.isoformat()}"
        )
        if isinstance(folder, Path):
            folder.mkdir(parents=True, exist_ok=True)
        return {
            "data": folder / f"data.{format}",
            "metadata": folder / "metadata.json",
        }

    def _save_parquet(self, dataframe: pd.DataFrame, target_path: AnyPath):
        """save a dataframe as parquet

        :param dataframe: dataframe to be saved as file
        :param target_path: the target file full path
        """
        if isinstance(target_path, S3Path):
            target_path = str(target_path)
            logger.debug(
                "Converting S3Path to string for pandas compatability\n"
                f"{type(target_path)}: {target_path}"
            )
        logger.debug(f"Saving parquet format to {target_path} ...")
        dataframe.to_parquet(target_path)

    def _save_csv(self, dataframe: pd.DataFrame, target_path: AnyPath):
        """save a dataframe as csv

        :param dataframe: dataframe to be saved as file
        :param target_path: the target file full path
        """
        if isinstance(target_path, S3Path):
            target_path = str(target_path)
            logger.debug(
                "Converting S3Path to string for pandas compatability\n"
                f"{type(target_path)}: {target_path}"
            )
        logger.debug(f"Saving csv format to {target_path} ...")
        dataframe.to_csv(target_path, index=False)

    def _save_metadata(self, metadata: Dict, target_path: AnyPath):
        """save metadata as a json file

        :param metadata: metadata in dictionary format
        :param target_path:
        """
        logger.debug(f"Saving metadata to {target_path} ...")
        with target_path.open("w+") as fp:
            json.dump(metadata, fp, indent=2)


class StoreJSON:
    """Save json data

    We follow the schema

    ```python
    target_folder / "format=parquet" / "snapshot_date="
    ```

    `target_folder` should reflect what data is inside.
    For example, we may use

    ```
    target_folder = some_folder / "google_trend"
    / "keyword=phone_case" / "cat=0"
    / "geo=de" / "timeframe=today-5-y"
    ```

    :param target_folder: parent folder for the data.
        Note that subfolders will be created inside it.
    :param snapshot_date: the date when the data was produced.
        Please stick to UTC date.
    """

    def __init__(
        self,
        target_folder: AnyPath,
        snapshot_date: Union[datetime.date, Literal["latest"]],
    ):
        self.target_folder = target_folder

        if not isinstance(snapshot_date, (datetime.date, str)):
            raise TypeError(
                f"snapshot_date provided is not date time: {type(snapshot_date)}"
            )
        else:
            self.snapshot_date = snapshot_date

    def save(
        self,
        records: Any,
        formats: Optional[List[Literal["json"]]] = ["json"],
    ):
        """
        Save the trend results

        :param trend_data: the object containing the dataframe and metadata
        :param formats: which formats to save as
        """
        if not isinstance(formats, list):
            formats = [formats]

        format_dispatcher = {
            "json": {
                "method": self._save_json,
                "path": self._file_path(format="json"),
            },
        }

        for f in formats:
            try:
                f_method = format_dispatcher[f]["method"]
                f_path_data = format_dispatcher[f]["path"]["data"]  # type: ignore

                f_method(records=records, target_path=f_path_data)  # type: ignore
            except Exception as e:
                logger.error(f"can not save format {f}: {e}")

    @property
    def snapshot_date_str(self):
        if isinstance(self.snapshot_date, datetime.date):
            return self.snapshot_date.isoformat()
        elif self.snapshot_date == "latest":
            return self.snapshot_date
        else:
            raise ValueError(f"snapshot_date {self.snapshot_date} is not supported")

    def _file_path(self, format: Literal["json"]) -> Dict[str, AnyPath]:
        """Compute the full path for the target file
        based on the format

        :param format: the file format to be used
        """
        folder = (
            self.target_folder
            / f"format={format}"
            / f"snapshot_date={self.snapshot_date_str}"
        )
        if isinstance(folder, Path):
            folder.mkdir(parents=True, exist_ok=True)
        return {"data": folder / f"data.{format}"}

    def _save_json(self, records: Dict, target_path: AnyPath):
        """save a dataframe as json

        :param records: records to be saved as file
        :param target_path: the target file full path
        """
        logger.debug(f"Saving json format to {target_path} ...")
        with target_path.open("w+") as fp:
            json.dump(records, fp)
