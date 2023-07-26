import datetime
import json
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from cloudpathlib import AnyPath
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
        dataframe.to_parquet(target_path)

    def _save_csv(self, dataframe: pd.DataFrame, target_path: AnyPath):
        """save a dataframe as csv

        :param dataframe: dataframe to be saved as file
        :param target_path: the target file full path
        """
        dataframe.to_csv(target_path, index=False)

    def _save_metadata(self, metadata: Dict, target_path: AnyPath):
        """save metadata as a json file

        :param metadata: metadata in dictionary format
        :param target_path:
        """

        with open(target_path, "w") as fp:
            json.dump(metadata, fp, indent=2)
