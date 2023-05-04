import re
from app.processing.gcloud.data_merge import is_constant_df
from app.processing.prometheus.data_aggregate import (
    AGGREGATE_OUTPUT_PATH,
    TARGET_METRIC_NAMES_PATH,
)
import json
import os
import pandas as pd

from app.processing.prometheus.data_preprocess import METRIC_PATH

MERGE_OUTPUT_PATH = os.path.join(METRIC_PATH, "prometheus_merged")


def get_same_row_in_df(df: pd.DataFrame, row: pd.Series) -> pd.Series:
    return df[df.apply(lambda series: series.equals(row), axis=1)]


def read_kpi_map(
    aggregate_output_path: str, exp_name: str, metric_index: int
) -> pd.DataFrame:
    kpi_map_path = os.path.join(
        aggregate_output_path, exp_name, f"metric-{metric_index}-kpi-map.json"
    )
    with open(kpi_map_path) as fp:
        kpi_map_list = json.load(fp)
    kpi_maps = [kpi_map["kpi"] for kpi_map in kpi_map_list]
    indices = [kpi_map["index"] for kpi_map in kpi_map_list]
    return pd.DataFrame(kpi_maps, index=indices)


def merge_same_metric(
    aggregate_output_path: str, exp_names: list, metric_index: int, df_metric_list: list
):
    df_unique_kpi_map = None
    df_metric_exp_list = []
    for exp_name in exp_names:
        df_exp_metric = pd.read_csv(
            os.path.join(aggregate_output_path, exp_name, f"metric-{metric_index}.csv")
        ).set_index("timestamp")
        df_kpi_map = read_kpi_map(exp_name, metric_index)
        # use first df as initial one
        if df_unique_kpi_map is None:
            df_unique_kpi_map = df_kpi_map
            df_metric_exp_list.append(df_exp_metric)
            continue
        for index, row in df_unique_kpi_map.iterrows():
            same_row = get_same_row_in_df(df_unique_kpi_map, row)
            if same_row.empty:
                df_unique_kpi_map = pd.concat([df_unique_kpi_map, row])
                new_index = get_same_row_in_df(df_unique_kpi_map, row).index
                old_column_names = [
                    col
                    for col in df_exp_metric.columns
                    if col.startswith(f"agg-kpi-{index}")
                ]
                new_column_names = [
                    re.sub(r"(agg-kpi-)[0-9]+(-.*)", f"\1{new_index}\2", col)
                    for col in old_column_names
                ]
                df_exp_metric.rename(
                    {
                        old_column_names[i]: new_column_names[i]
                        for i in range(len(old_column_names))
                    },
                    inplace=True,
                )
        df_metric_exp_list.append(df_exp_metric)
    df_metric = pd.concat(df_metric_exp_list).add_prefix(f"pm-{metric_index}-")
    df_metric.index = pd.to_datetime(df_metric.index)
    if len(df_metric.index) != len(df_metric.index.drop_duplicates()):
        df_metric = df_metric.groupby(df_metric.index).agg("mean")
    df_metric.sort_index(inplace=True)
    df_metric.fillna(0, inplace=True)
    if is_constant_df(df_metric):
        print(f"Metric {metric_index} contains only constant time series!")
    else:
        df_unique_kpi_map.to_csv(
            os.path.join(MERGE_OUTPUT_PATH, f"metric-{metric_index}-kpi-map.csv")
        )
        df_metric.to_csv(os.path.join(MERGE_OUTPUT_PATH, f"metric-{metric_index}.csv"))
        df_metric_list.append(df_metric)


def merge_metrics(merge_output_path: str, aggregate_output_path: str):
    if not os.path.exists(merge_output_path):
        os.mkdir(merge_output_path)
    exp_names = [
        folder
        for folder in os.listdir(aggregate_output_path)
        if folder.startswith("day")
    ]
    exp_names.sort()
    df_metric_names = pd.read_csv(TARGET_METRIC_NAMES_PATH)
    df_metric_list = []
    for metric_index in df_metric_names.index:
        print(f"Processing metric {metric_index} ...")
        merge_same_metric(exp_names, metric_index, df_metric_list)
    df_complete = pd.concat(df_metric_list, axis=1)
    num_col = len(df_complete.columns)
    num_row = len(df_complete)
    print(f"{num_row} rows x {num_col} columns")
    df_complete.to_csv(os.path.join(MERGE_OUTPUT_PATH, "complete-time-series.csv"))


def merge_individual_metric(merge_output_path: str, aggregate_output_path: str):
    if not os.path.exists(merge_output_path):
        os.mkdir(merge_output_path)
    df_metric_names = pd.read_csv(TARGET_METRIC_NAMES_PATH)
    df_metric_list = []
    for metric_index in df_metric_names.index:
        print(f"Processing metric {metric_index} ...")
        merge_same_metric(aggregate_output_path, [""], metric_index, df_metric_list)
    df_complete = pd.concat(df_metric_list, axis=1)
    num_col = len(df_complete.columns)
    num_row = len(df_complete)
    print(f"{num_row} rows x {num_col} columns")
    df_complete.to_csv(os.path.join(merge_output_path, "prom-complete-time-series.csv"))
