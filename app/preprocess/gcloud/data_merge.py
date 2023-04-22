import os
import pandas as pd

GCLOUD_METRICS_PATH = "gcloud-metrics"

GCLOUD_AGGREGATED_PATH = os.path.join(GCLOUD_METRICS_PATH, "gcloud_aggregated")


def get_metric_indices() -> list:
    return [
        filename.lstrip("metric-").rstrip("-kpi-map.json")
        for filename in os.listdir(GCLOUD_AGGREGATED_PATH)
        if filename.endswith("kpi-map.json")
    ]


def is_constant_df(df: pd.DataFrame) -> bool:
    return ((df == df.iloc[0]).all()).all()


def remove_constants_and_merge():
    df_all_list = []
    for metric_index in get_metric_indices():
        print(f"Processing metric {metric_index} ...")
        metric_path = os.path.join(GCLOUD_AGGREGATED_PATH, f"metric-{metric_index}.csv")
        df_metric = pd.read_csv(metric_path).set_index("timestamp")
        if is_constant_df(df_metric):
            # found metric 12 and 13 contain only constants in normal time series
            print(f"Metric {metric_index} contains only constant time series!")
        else:
            df_all_list.append(df_metric.add_prefix(f"metric-{metric_index}-"))
    df_all = pd.concat(df_all_list, axis=1)
    df_all.to_csv(os.path.join(GCLOUD_METRICS_PATH, "complete-time-series.csv"))


def inspect_merged_df():
    df_all = pd.read_csv(
        os.path.join(GCLOUD_METRICS_PATH, "complete-time-series.csv")
    ).set_index("timestamp")
    num_col = len(df_all.columns)
    num_row = len(df_all)
    print(f"{num_row} rows x {num_col} columns")


def main():
    # remove_constants_and_merge()
    inspect_merged_df()


if __name__ == "__main__":
    main()
