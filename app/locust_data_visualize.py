import os
import matplotlib.pyplot as plt
import pandas as pd
from app.locust_data_preprocess import agg_stats_to_minute
from datetime import datetime


def visualize_response_time(
    figprefix: str,
    df_normal_stats: pd.DataFrame,
    df_stats: pd.DataFrame = None,
    failure_start: int = None,
    failure_end: int = None,
):
    if df_stats is not None:
        response_time = df_stats["locust-95%"].to_list()
    else:
        response_time = df_normal_stats["locust-95%"].to_list()
    mean_response_time = df_normal_stats["locust-95%"].mean()
    std3_response_time = df_normal_stats["locust-95%"].std() * 3
    print("mean is", mean_response_time)
    print("3 times std is", std3_response_time)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(response_time, label="normal")
    ax.axhline(mean_response_time, color="yellowgreen", label="mean")
    ax.axhline(std3_response_time, color="r", label="3 * std")
    if failure_start and failure_end:
        ax.axvline(failure_start, color="y", linestyle="dashed", label="failure")
        ax.axvline(failure_end, color="y", linestyle="dashed")
    ax.set_ylabel("response time (ms)")
    ax.set_xlabel("minutes")
    ax.legend()
    plt.savefig(figprefix + "_response_time")


def visualize_failures(
    figprefix: str,
    df_normal_stats: pd.DataFrame,
    df_stats: pd.DataFrame = None,
    failure_start: int = None,
    failure_end: int = None,
):
    if df_stats is not None:
        response_time = df_stats["locust-Failures/s"].to_list()
    else:
        response_time = df_normal_stats["locust-Failures/s"].to_list()
    mean_response_time = df_normal_stats["locust-Failures/s"].mean()
    std3_response_time = df_normal_stats["locust-Failures/s"].std() * 3
    print("mean is", mean_response_time)
    print("3 times std is", std3_response_time)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(response_time, label="normal")
    ax.axhline(mean_response_time, color="yellowgreen", label="mean")
    ax.axhline(std3_response_time, color="r", label="3 * std")
    if failure_start and failure_end:
        ax.axvline(failure_start, color="y", linestyle="dashed", label="failure scope")
        ax.axvline(failure_end, color="y", linestyle="dashed")
    ax.set_ylabel("failures/s")
    ax.set_xlabel("minutes")
    ax.legend()
    plt.savefig(figprefix + "_failures")


def draw_normal(
    df_original_normal_stats: pd.DataFrame, df_cleaned_normal_stats: pd.DataFrame
):
    visualize_response_time("original_normal", df_original_normal_stats)
    visualize_failures("original_normal", df_original_normal_stats)
    visualize_response_time("cleaned_normal", df_cleaned_normal_stats)
    visualize_failures("cleaned_normal", df_cleaned_normal_stats)


def get_index_from_df_by_time(df: pd.DataFrame, time: str) -> int:
    df = df.reset_index()
    time_dt = datetime.utcfromtimestamp(datetime.fromisoformat(time).timestamp())
    df_index = df[
        (df["month"] == time_dt.month)
        & (df["day"] == time_dt.day)
        & (df["hour"] == time_dt.hour)
        & (df["minute"] == time_dt.minute)
    ].index
    return df_index[0]


def draw_failure(
    failure_name: str,
    df_failure_stats: pd.DataFrame,
    df_cleaned_normal_stats: pd.DataFrame,
    failure_start: str,
    failure_end: str,
):
    start_index = get_index_from_df_by_time(df_failure_stats, failure_start)
    end_index = get_index_from_df_by_time(df_failure_stats, failure_end)
    visualize_response_time(
        failure_name, df_cleaned_normal_stats, df_failure_stats, start_index, end_index
    )
    visualize_failures(
        failure_name, df_cleaned_normal_stats, df_failure_stats, start_index, end_index
    )


def main():
    stats_dir_path = (
        "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/normal"
    )
    original_agg_stats_path = os.path.join(
        stats_dir_path, "locust_original_agg_stats.csv"
    )
    cleaned_agg_stats_path = os.path.join(
        stats_dir_path, "locust_cleaned_agg_stats.csv"
    )
    df_original_normal_stats = pd.read_csv(original_agg_stats_path)
    df_cleaned_normal_stats = pd.read_csv(cleaned_agg_stats_path)
    draw_normal(df_original_normal_stats, df_cleaned_normal_stats)
    print(
        len(df_original_normal_stats),
        "reduced to",
        len(df_cleaned_normal_stats),
        "by removing outliers.",
    )

    df_failure_stats = agg_stats_to_minute(
        "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/failure injection/day-8-constant-network-corrupt-userapi/alemira_stats_history.csv"
    )
    draw_failure(
        "day-8-linear-network-loss-userapi",
        df_failure_stats,
        df_cleaned_normal_stats,
        "2023-04-21T14:30:00+02:00",
        "2023-04-21T16:30:00+02:00",
    )


if __name__ == "__main__":
    main()
