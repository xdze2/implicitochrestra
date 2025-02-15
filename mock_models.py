import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

import pandas as pd
import numpy as np
from pathlib import Path


def _generate_mock_timeseries(start="2024-01-01", end="2024-12-31", freq="D", seed=42):
    """Generate a mock time series DataFrame with temperature and humidity."""

    np.random.seed(seed)
    date_range = pd.date_range(start=start, end=end, freq=freq)  # Generate time index

    temperature = np.random.uniform(low=-5, high=35, size=len(date_range))
    humidity = np.random.uniform(low=20, high=90, size=len(date_range))

    df = pd.DataFrame(
        {"temperature": np.round(temperature, 2), "humidity": np.round(humidity, 2)},
        index=date_range,
    )

    df.index.name = "date"  # Naming the index
    return df


def fetch_timeserie_data(
    timeserie_name: str, entity_id: str, start_date: str, end_date: str
) -> pd.DataFrame:  #
    """Fetch data from API and save as Parquet file."""

    df = _generate_mock_timeseries(start=start_date, end=end_date, freq="D", seed=42)
    return df


def save_timeserie_data(
    output_dir: str,
    timeserie_name: str,
    entity_id: str,
    df: pd.DataFrame,
    start_date: str = None,
    end_date: str = None,
) -> str:
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # start_iso = datetime.strptime(start_date, "%Y-%m-%d").isoformat()
    # end_iso = datetime.strptime(end_date, "%Y-%m-%d").isoformat()

    output_filename = f"{timeserie_name}_{entity_id}.parquet"
    output_filepath = Path(output_dir, output_filename)
    df.to_parquet(output_filepath, index=False)

    print(f"Saved: {output_filepath}")
    return output_filepath


def make_graph(output_dir: str, df: pd.DataFrame) -> str:
    """Generate a graph."""

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Ensure datetime column exists and is parsed
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

    # Plot data
    plt.figure(figsize=(10, 5))
    plt.plot(df.index, df.iloc[:, 0], label="Value", marker="o", linestyle="-")
    plt.xlabel("Time")
    plt.ylabel("Measurement")
    plt.title("Time Series Data")
    plt.legend()
    plt.grid()

    # Save graph
    filepath = Path(output_dir, "graph.png")
    plt.savefig(filepath)
    print(f"Graph saved: {filepath}")
    return filepath
