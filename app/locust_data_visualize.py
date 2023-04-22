import os
import matplotlib.pyplot as plt
import pandas as pd
from app.preprocess.locust.data_preprocess import agg_stats_to_minute
from datetime import datetime


def visualize_response_time(
    output_path: str,
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
        ax.axvline(failure_start, color="m", linestyle="dashed", label="failure")
        ax.axvline(failure_end, color="m", linestyle="dashed")
    ax.set_ylabel("response time (ms)")
    ax.set_xlabel("minutes")
    ax.legend()
    plt.savefig(os.path.join(output_path, figprefix + "_response_time"))


def visualize_failures(
    output_path: str,
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
        ax.axvline(failure_start, color="m", linestyle="dashed", label="failure scope")
        ax.axvline(failure_end, color="m", linestyle="dashed")
    ax.set_ylabel("failures/s")
    ax.set_xlabel("minutes")
    ax.legend()
    plt.savefig(os.path.join(output_path, figprefix + "_failures"))


def draw_normal(
    df_original_normal_stats: pd.DataFrame,
    df_cleaned_normal_stats: pd.DataFrame,
    output_path: str,
):
    visualize_response_time(output_path, "original_normal", df_original_normal_stats)
    visualize_failures(output_path, "original_normal", df_original_normal_stats)
    visualize_response_time(output_path, "cleaned_normal", df_cleaned_normal_stats)
    visualize_failures(output_path, "cleaned_normal", df_cleaned_normal_stats)
    print(
        len(df_original_normal_stats),
        "reduced to",
        len(df_cleaned_normal_stats),
        "by removing outliers.",
    )


def get_index_from_df_by_time(df: pd.DataFrame, time: str) -> int:
    time_dt = datetime.utcfromtimestamp(datetime.fromisoformat(time).timestamp())
    df_index = df[df["timestamp"] == time_dt].index
    return df_index[0]


def draw_failure(
    failure_name: str,
    df_failure_stats: pd.DataFrame,
    df_cleaned_normal_stats: pd.DataFrame,
    failure_start: str,
    failure_end: str,
    output_path: str,
):
    start_index = get_index_from_df_by_time(df_failure_stats, failure_start)
    end_index = get_index_from_df_by_time(df_failure_stats, failure_end)
    visualize_response_time(
        output_path,
        failure_name,
        df_cleaned_normal_stats,
        df_failure_stats,
        start_index,
        end_index,
    )
    visualize_failures(
        output_path,
        failure_name,
        df_cleaned_normal_stats,
        df_failure_stats,
        start_index,
        end_index,
    )


def main():
    output_path = "visualizations"
    if not os.path.exists(output_path):
        os.mkdir(output_path)
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
    # draw_normal(df_original_normal_stats, df_cleaned_normal_stats, output_path)

    failure_dir = "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/failure injection"
    failures = [
        (
            "day-8-constant-network-corrupt-userapi",
            "2023-04-21T14:30:00+02:00",
            "2023-04-21T16:30:00+02:00",
        ),
        (
            "day-8-constant-network-loss-userapi",
            "2023-04-21T17:55:00+02:00",
            "2023-04-21T19:55:00+02:00",
        ),
        (
            "day-8-constant-network-delay-userapi",
            "2023-04-22T07:55:00+02:00",
            "2023-04-22T09:55:00+02:00",
        ),
        (
            "day-8-constant-cpu-stress-userapi",
            "2023-04-22T15:24:00+02:00",
            "2023-04-22T17:24:00+02:00",
        ),
    ]
    for failure in failures[-1:]:
        failure_path = os.path.join(
            failure_dir, failure[0], "alemira_stats_history.csv"
        )
        df_failure_stats = agg_stats_to_minute(failure_path)
        draw_failure(
            failure[0],
            df_failure_stats,
            df_cleaned_normal_stats,
            failure[1],
            failure[2],
            output_path,
        )


if __name__ == "__main__":
    main()
