import os
import matplotlib.pyplot as plt
import pandas as pd
from app.processing.locust.data_preprocess import agg_stats_to_minute
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
        threshold_label = "max normal"
        response_time = df_stats["lm-95%"].to_list()
        threshold_response_time = df_normal_stats["lm-95%"].max()
    else:
        threshold_label = "quantile 99.5%"
        threshold_response_time = df_normal_stats["lm-95%"].quantile(0.995)
        response_time = df_normal_stats["lm-95%"].to_list()
    mean_response_time = df_normal_stats["lm-95%"].mean()

    # percent_above_std3 = (
    #     100
    #     * len(df_normal_stats[df_normal_stats["lm-95%"] > threshold_response_time])
    #     / len(df_normal_stats)
    # )
    # print(f"{percent_above_std3}% response time above std3")
    print("mean is", mean_response_time)
    print("threshold is", threshold_response_time)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(response_time, label="normal")
    ax.axhline(mean_response_time, color="yellowgreen", label="mean")
    ax.axhline(threshold_response_time, color="r", label=threshold_label)
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
        threshold_label = "max normal"
        failures_rate = df_stats["lm-Failures/s"].to_list()
        threshold_failures_rate = df_normal_stats["lm-Failures/s"].max()
    else:
        threshold_label = "quantile 99.5%"
        threshold_failures_rate = df_normal_stats["lm-Failures/s"].std() * 3
        failures_rate = df_normal_stats["lm-Failures/s"].to_list()
    mean_failures_rate = df_normal_stats["lm-Failures/s"].mean()

    print("mean is", mean_failures_rate)
    print("threshold is", threshold_failures_rate)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(failures_rate, label="normal")
    ax.axhline(mean_failures_rate, color="yellowgreen", label="mean")
    ax.axhline(threshold_failures_rate, color="r", label=threshold_label)
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
    round_time_dt = pd.Timestamp(time_dt).round("min")
    df_index = df[df["timestamp"] == round_time_dt].index
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
    # original_agg_stats_path = os.path.join(
    #     stats_dir_path, "locust_original_agg_stats.csv"
    # )
    cleaned_agg_stats_path = os.path.join(stats_dir_path, "locust_normal_stats.csv")
    # df_original_normal_stats = pd.read_csv(original_agg_stats_path)
    df_cleaned_normal_stats = pd.read_csv(cleaned_agg_stats_path)
    # draw_normal(df_original_normal_stats, df_cleaned_normal_stats, output_path)
    # visualize_response_time(output_path, "cleaned_normal", df_cleaned_normal_stats)
    # visualize_failures(output_path, "cleaned_normal", df_cleaned_normal_stats)

    failure_dir = "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/failure injection"
    failure_injection_log_path = os.path.join(
        failure_dir, "alemira failure injection log", "log-failure-injection.csv"
    )
    df_failure_injection_log = pd.read_csv(failure_injection_log_path)
    failures = []
    for _, row in df_failure_injection_log.iterrows():
        failures.append((row["Folder Name"], row["Failure Begin"], row["Failure End"]))
    for failure in failures:
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
