from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict
import uuid

from rich.pretty import pprint
from dataclasses import dataclass, field

# import networkx as nx

import time
import json

import pandas as pd


from .implicit import MetaTask, TaskRun


class FetchDeviceData(MetaTask):
    def __init__(self, entity_id: str, timeserie_name: str):
        self.entity_id = entity_id
        self.timeserie_name = timeserie_name

    def get_hash(self) -> str:
        return f"{self.__class__.__name__}:{self.entity_id}:{self.timeserie_name}"

    def compute(self, run: TaskRun):
        from mock_models import fetch_timeserie_data, save_timeserie_data

        ts_data = fetch_timeserie_data(
            timeserie_name=self.timeserie_name,
            entity_id=self.entity_id,
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        output_filename = save_timeserie_data(
            run.get_output_dir(),
            timeserie_name=self.timeserie_name,
            entity_id=self.entity_id,
            df=ts_data,
        )
        run.register_output("dataframe_file", str(output_filename))
        self.result = [1, 2, 3]
        run.log_info(f"fetching data for {self.entity_id}")

    @staticmethod
    def load_dataframe(filepth: str) -> pd.DataFrame:
        return pd.read_parquet(filepth)


class GraphWeight(MetaTask):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id

    def get_hash(self) -> str:
        return f"{self.__class__.__name__}:{self.entity_id}"

    def compute(self, run: TaskRun):
        from mock_models import make_graph

        task1_data1 = FetchDeviceData(
            entity_id=self.entity_id, timeserie_name="temperature"
        )
        last_task1_run = task1_data1.get()
        run.register_input(last_task1_run)
        data1 = task1_data1.load_dataframe(last_task1_run.outputs["dataframe_file"])

        # task2_data2 = FetchDeviceData(entity_id=34, timeserie_name="weight")
        # run.register_input(task2_data2)
        # data1 = task1_data1.get()
        # data2 = task2_data2.get()

        graph_path = make_graph(run.get_output_dir(), data1)
        run.register_output("graph_file", str(graph_path))
