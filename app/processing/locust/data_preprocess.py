import os
from scipy.stats import zscore
import numpy as np
import pandas as pd

METRIC_PATH = "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/normal"


def agg_stats_to_minute(stats_path: str) -> pd.DataFrame:
    df_stats = pd.read_csv(stats_path)
    df_stats = df_stats[df_stats["Name"] == "Aggregated"].drop(columns=["Type", "Name"])
    df_stats["Timestamp"] = pd.to_datetime(df_stats["Timestamp"], unit="s").dt.round(
        "min"
    )
    df_stats = df_stats.groupby("Timestamp").agg(
        {
            "User Count": "max",
            "Requests/s": "mean",
            "Failures/s": "mean",
            "50%": "max",
            "66%": "max",
            "75%": "max",
            "80%": "max",
            "90%": "max",
            "95%": "max",
            "98%": "max",
            "99%": "max",
            "99.9%": "max",
            "99.99%": "max",
            "100%": "max",
            "Total Request Count": "sum",
            "Total Failure Count": "sum",
            "Total Median Response Time": "median",
            "Total Average Response Time": "mean",
            "Total Min Response Time": "min",
            "Total Max Response Time": "max",
            "Total Average Content Size": "mean",
        }
    )
    if len(df_stats) > 60 * 24:
        df_stats = df_stats[1:1441]
    df_stats.index.rename("timestamp", inplace=True)
    df_stats = df_stats.add_prefix("lm-").reset_index()
    return df_stats
