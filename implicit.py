from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict
import uuid

from rich.pretty import pprint
from dataclasses import dataclass, field

# import networkx as nx

import time
import json


RUN_DIR = "out/runs"
DATA_DIR = "out/data"


@dataclass
class TaskRun:
    """Metadata about a task computation (a run)."""

    task_hash: str
    run_id: str = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    input_runs: Dict[str, str] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    logs: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.run_id = (
            self.task_hash + str(uuid.uuid4()) if self.run_id is None else self.run_id
        )
        self.start_time = time.time() if self.start_time is None else self.start_time
        self.end_time = None
        self._run_path = self.get_run_path(self.task_hash)

    def save(self):
        self.end_time = time.time()
        Path(RUN_DIR).mkdir(parents=True, exist_ok=True)
        as_dict = {
            "task_hash": self.task_hash,
            "run_id": self.run_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "input_runs": self.input_runs,
            "outputs": self.outputs,
            "logs": self.logs,
        }
        with open(self._run_path, "w") as file:
            json.dump(as_dict, file, indent=2)
        print(f"run saved to {self._run_path}")

    @classmethod
    def from_last_run(cls, task_hash: str) -> Optional["TaskRun"]:
        _run_path = cls.get_run_path(task_hash)
        if not Path(_run_path).exists():
            return None

        try:
            with open(_run_path, "r") as f:
                data = json.load(f)
                return cls(**data)
        except json.decoder.JSONDecodeError as err:
            print(f"Error while loading {_run_path}: {err}. Ignoring previous run.")
            return None

    @staticmethod
    def get_run_path(task_hash) -> Path:
        return Path(RUN_DIR, f"{task_hash}.json")

    def get_output_dir(self) -> Path:
        return Path(DATA_DIR, self.task_hash)

    def register_input(self, input_run: "TaskRun") -> None:
        self.input_runs[input_run.task_hash] = input_run.run_id

    def register_output(self, key: str, value: Any) -> None:
        self.outputs[key] = value

    def log_info(self, msg):
        self.logs.append((time.time(), "INFO", msg))
        print(f"{self.task_hash}: {msg}")


class MetaTask:
    """Abstract class for a task (a Model) that can be run."""

    def get_hash(self) -> str:
        """Return the task identifier, including parameters."""
        raise NotImplementedError

    def compute(self, run: TaskRun):
        """Busness logic of the task."""
        raise NotImplementedError

    def run(self):
        """Do the task computation."""
        task_hash = self.get_hash()
        run = TaskRun(task_hash=task_hash)
        self.compute(run)
        run.save()
        return run

    def get_last_run(self):
        task_hash = self.get_hash()
        print("task_hash", task_hash)
        last_run = TaskRun.from_last_run(task_hash=task_hash)
        return last_run

    def _is_cache_valid(self, last_run: TaskRun) -> bool:
        """Check if the cache is still valid."""

        if last_run is None:
            print("not cached run")
            return False

        pprint(last_run.input_runs)
        for task_id, run_id in last_run.input_runs.items():
            parent_run = TaskRun.from_last_run(task_hash=task_id)
            if parent_run is None:
                print(f"missing parent run {task_id}")
                return False
            if parent_run.run_id != run_id:
                print(f"parent run {task_id} is not up to date")
                return False

        return True

    def get(self) -> TaskRun:
        """Get the result, either from cache or from computation."""
        last_run = self.get_last_run()
        if self._is_cache_valid(last_run):
            print(f"<GET> task={self.get_hash()} cached")
            return last_run
        else:
            print(f"<RUN> task={self.get_hash()}")
            current_run = self.run()
            return current_run


import pandas as pd


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


graph = GraphWeight(entity_id=12354)

last_run = graph.get()

print(last_run)
