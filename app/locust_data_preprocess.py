import os
from scipy.stats import zscore
import numpy as np
import pandas as pd

METRIC_PATH = "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/normal"


def agg_stats_to_minute(stats_path: str) -> pd.DataFrame:
    df_stats = pd.read_csv(stats_path)
    df_stats = df_stats[df_stats["Name"] == "Aggregated"].drop(columns=["Type", "Name"])
    df_stats["Timestamp"] = pd.to_datetime(df_stats["Timestamp"], unit="s")
    df_stats = df_stats.groupby(
        [
            df_stats["Timestamp"].dt.month,
            df_stats["Timestamp"].dt.day,
            df_stats["Timestamp"].dt.hour,
            df_stats["Timestamp"].dt.minute,
        ]
    ).agg(
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
    df_stats.index.rename(["month", "day", "hour", "minute"], inplace=True)
    if len(df_stats) > 60 * 24:
        df_stats = df_stats[1:1441]
    return df_stats.add_prefix("locust-")


def clean_normal_stats(df: pd.DataFrame):
    df = df[(np.abs(zscore(df[["locust-Failures/s", "locust-95%"]])) < 3).all(axis=1)]
    df.to_csv(os.path.join(METRIC_PATH, "locust_cleaned_agg_stats.csv"))


def merge_stats():
    df_list = []
    for folder in os.listdir(METRIC_PATH):
        if folder.startswith("day-"):
            stats_path = os.path.join(METRIC_PATH, folder, "alemira_stats_history.csv")
            df_stats = agg_stats_to_minute(stats_path)
            df_list.append(df_stats)
    complete_df = pd.concat(df_list)
    complete_df.to_csv(os.path.join(METRIC_PATH, "locust_original_agg_stats.csv"))


def main():
    # merge_stats()
    df = pd.read_csv(os.path.join(METRIC_PATH, "locust_original_agg_stats.csv"))
    clean_normal_stats(df)


if __name__ == "__main__":
    main()
