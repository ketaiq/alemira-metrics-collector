# Aggregate each metric from each day and then merge all 14 days' time series
import json
import os
import warnings
import numpy as np
import pandas as pd
from app.processing.prometheus.data_preprocess import (
    METRIC_PATH,
    Metric,
    get_exp_names,
    get_metric,
)

TARGET_METRIC_NAMES_PATH = os.path.join(METRIC_PATH, "target_metrics.csv")
AGGREGATE_OUTPUT_PATH = os.path.join(METRIC_PATH, "prometheus_aggregated")


def index_list(series) -> list:
    return series.to_list()


def aggregate_alerts_time_series(
    metric_index: int, agg_exp_path: str, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
):
    # ALERTS contain only counts of alerts
    # drop KPIs with namespace not alms
    useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
    for i in useless_kpi_indices:
        df_kpi.drop(columns=f"value-{i}", inplace=True)
    df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
    # create group columns for aggregation
    group_columns = ["alertname", "alertstate", "container"]
    df_kpi_map["container"] = df_kpi_map["container"].fillna("undefined")
    df_kpi_map = df_kpi_map[group_columns]
    # group indices by labels
    df_kpi_indices_to_agg = (
        df_kpi_map.reset_index().groupby(group_columns).agg(index_list)
    )
    aggregate(
        metric_index,
        agg_exp_path,
        df_kpi_indices_to_agg,
        df_kpi.fillna(0),
        ["sum", "count"],
    )


def aggregate_alerts_for_state_time_series(
    metric_index: int, agg_exp_path: str, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
):
    # ALERTS_FOR_STATE contain only timestamps of alerts
    # drop KPIs with namespace not alms
    useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
    for i in useless_kpi_indices:
        df_kpi.drop(columns=f"value-{i}", inplace=True)
    df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
    # transform df_kpi to boolean values according to timestamps in values
    df_kpi = df_kpi.notnull().astype("int")
    # create group columns for aggregation
    group_columns = ["alertname", "container"]
    df_kpi_map["container"] = df_kpi_map["container"].fillna("undefined")
    df_kpi_map = df_kpi_map[group_columns]
    # group indices by labels
    df_kpi_indices_to_agg = (
        df_kpi_map.reset_index().groupby(group_columns).agg(index_list)
    )
    aggregate(
        metric_index,
        agg_exp_path,
        df_kpi_indices_to_agg,
        df_kpi,
        ["sum", "count"],
    )


def aggregate_container_time_series(
    metric_index: int, agg_exp_path: str, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
):
    # drop KPIs with namespace not alms
    useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
    for i in useless_kpi_indices:
        df_kpi.drop(columns=f"value-{i}", inplace=True)
    df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
    # create group columns for aggregation
    if "container" in df_kpi_map:
        group_columns = ["container"]
        df_kpi_map = df_kpi_map[group_columns]
    elif "pod" in df_kpi_map:
        df_kpi_map["pod_service"] = df_kpi_map["pod"].str.extract(r"(alms[-a-z]+)-")
        group_columns = ["pod_service"]
        df_kpi_map = df_kpi_map[group_columns]
    else:
        print(f"\tNo labels can be aggregated!")
    # group indices by labels
    df_kpi_indices_to_agg = (
        df_kpi_map.reset_index().groupby(group_columns).agg(index_list)
    )
    aggregate(
        metric_index,
        agg_exp_path,
        df_kpi_indices_to_agg,
        df_kpi,
        [
            "min",
            "max",
            "mean",
            "median",
            "std",
            "count",
        ],
    )


def aggregate_time_series_in_one(
    metric_index: int, agg_exp_path: str, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
):
    isunique = df_kpi_map.nunique() == 1
    df_kpi_map = df_kpi_map[isunique.index[isunique]]
    group_columns = df_kpi_map.columns.to_list()
    df_kpi_indices_to_agg = (
        df_kpi_map.reset_index().groupby(group_columns).agg(index_list)
    )
    aggregate(
        metric_index,
        agg_exp_path,
        df_kpi_indices_to_agg,
        df_kpi,
        [
            "min",
            "max",
            "mean",
            "median",
            "std",
            "count",
        ],
    )


def aggregate_kube_time_series(
    metric_index: int, agg_exp_path: str, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
):
    # drop KPIs with namespace not alms
    useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
    for i in useless_kpi_indices:
        df_kpi.drop(columns=f"value-{i}", inplace=True)
    df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
    # drop useless labels
    is_singleton_label = df_kpi_map.nunique() == 1
    df_kpi_map = df_kpi_map.drop(columns=is_singleton_label.index[is_singleton_label])
    known_useless_labels = ["instance", "pod", "status", "condition"]
    for label in known_useless_labels:
        if label in df_kpi_map.columns:
            df_kpi_map.drop(columns=label, inplace=True)
    # create group columns for aggregation
    group_columns = df_kpi_map.columns.to_list()
    df_kpi_map = df_kpi_map[group_columns]
    # group indices by labels
    df_kpi_indices_to_agg = (
        df_kpi_map.reset_index().groupby(group_columns).agg(index_list)
    )
    aggregate(
        metric_index,
        agg_exp_path,
        df_kpi_indices_to_agg,
        df_kpi,
        [
            "min",
            "max",
            "mean",
            "median",
            "std",
            "count",
        ],
    )


def adapt_no_agg_time_series(
    metric_index: int, agg_exp_path: str, df_kpi_map: pd.DataFrame, df_kpi: pd.DataFrame
):
    # drop KPIs with namespace not alms
    if "namespace" in df_kpi_map.columns:
        useless_kpi_indices = df_kpi_map[df_kpi_map["namespace"] != "alms"].index
        for i in useless_kpi_indices:
            df_kpi.drop(columns=f"value-{i}", inplace=True)
        df_kpi_map = df_kpi_map.drop(useless_kpi_indices)
    # copy time series
    new_kpi_map_list = []
    new_kpi_map_index = 1
    for i in df_kpi_map.index:
        kpi_keys = df_kpi_map.columns
        kpi_values = df_kpi_map.loc[i]
        kpi = {kpi_keys[i]: kpi_values[i] for i in range(len(kpi_keys))}
        new_kpi_map = {"index": new_kpi_map_index, "kpi": kpi}
        new_kpi_map_list.append(new_kpi_map)
        df_kpi.rename(
            columns={f"value-{i}": f"agg-kpi-{new_kpi_map_index}"}, inplace=True
        )
    if not df_kpi.empty:
        with open(
            os.path.join(agg_exp_path, f"metric-{metric_index}-kpi-map.json"),
            "w",
        ) as fp:
            json.dump(new_kpi_map_list, fp)
        df_kpi.to_csv(os.path.join(agg_exp_path, f"metric-{metric_index}.csv"))


def aggregate(
    metric_index: int,
    agg_exp_path: str,
    df_kpi_indices_to_agg: pd.DataFrame,
    df_kpi: pd.DataFrame,
    aggregate_funcs: list,
):
    new_kpi_map_list = []
    new_kpi_map_index = 1
    df_agg_list = []
    for row in df_kpi_indices_to_agg.itertuples():
        # generate new KPI
        kpi_keys = df_kpi_indices_to_agg.index.names
        kpi_values = row[0]
        if type(kpi_values) is not tuple:
            kpi_values = [kpi_values]
        kpi = {kpi_keys[i]: kpi_values[i] for i in range(len(kpi_keys))}
        new_kpi_map = {"index": new_kpi_map_index, "kpi": kpi}

        # generate indices to be grouped
        indices_in_df_kpi = [
            int(column.lstrip("value-")) for column in df_kpi.columns.to_list()
        ]
        valid_indices = list(set(row[1]) & set(indices_in_df_kpi))
        columns_to_agg = [f"value-{i}" for i in valid_indices]

        if len(columns_to_agg) == 1:
            new_kpi_map_list.append(new_kpi_map)
            single_column_name = columns_to_agg[0]
            df_agg_list.append(
                df_kpi[single_column_name].rename(f"agg-kpi-{new_kpi_map_index}")
            )
            new_kpi_map_index += 1
            continue
        elif len(columns_to_agg) == 0:
            continue
        new_kpi_map_list.append(new_kpi_map)
        # merge more than 1 columns
        df_metric_to_agg = df_kpi[columns_to_agg]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            df_metric_agg = df_metric_to_agg.agg(aggregate_funcs, axis=1).add_prefix(
                f"agg-kpi-{new_kpi_map_index}-"
            )
        df_agg_list.append(df_metric_agg)
        new_kpi_map_index += 1
    df_complete_agg = pd.concat(df_agg_list, axis=1)
    if not df_complete_agg.empty:
        with open(
            os.path.join(agg_exp_path, f"metric-{metric_index}-kpi-map.json"),
            "w",
        ) as fp:
            json.dump(new_kpi_map_list, fp)
        df_complete_agg.to_csv(os.path.join(agg_exp_path, f"metric-{metric_index}.csv"))


def aggregate_metric(
    metric_name: str,
    metric_index: int,
    agg_exp_path: str,
    df_kpi_map: pd.DataFrame,
    df_kpi: pd.DataFrame,
):
    if metric_name == "ALERTS":
        aggregate_alerts_time_series(metric_index, agg_exp_path, df_kpi_map, df_kpi)
    elif metric_name == "ALERTS_FOR_STATE":
        aggregate_alerts_for_state_time_series(
            metric_index, agg_exp_path, df_kpi_map, df_kpi
        )
    elif metric_name.startswith("container"):
        aggregate_container_time_series(metric_index, agg_exp_path, df_kpi_map, df_kpi)
    elif metric_name.startswith("instance") or metric_name.startswith("node_"):
        aggregate_time_series_in_one(metric_index, agg_exp_path, df_kpi_map, df_kpi)
    elif metric_name.startswith("kube"):
        aggregate_kube_time_series(metric_index, agg_exp_path, df_kpi_map, df_kpi)
    elif (
        metric_name.startswith("namespace")
        or metric_name.startswith(":node")
        or metric_name.startswith("node:")
    ):
        adapt_no_agg_time_series(metric_index, agg_exp_path, df_kpi_map, df_kpi)


def aggregate_by_day():
    if not os.path.exists(AGGREGATE_OUTPUT_PATH):
        os.mkdir(AGGREGATE_OUTPUT_PATH)
    df_metric_names = pd.read_csv(TARGET_METRIC_NAMES_PATH)
    metric_names = df_metric_names["name"]
    exp_names = get_exp_names()
    num_metrics = len(metric_names)
    for metric_name in metric_names:
        metric_index = df_metric_names[df_metric_names["name"] == metric_name].index[0]
        print(f"Processing {metric_index+1}/{num_metrics} {metric_name} ...")
        for exp_name in exp_names:
            print(f"\ton {exp_name} ...")
            # create experiment folder if not exists
            agg_exp_path = os.path.join(AGGREGATE_OUTPUT_PATH, exp_name)
            if not os.path.exists(agg_exp_path):
                os.mkdir(agg_exp_path)
            metric = get_metric(exp_name, metric_name)
            kpi_map_list = []  # contains each metadata from metric items
            metric_items_df_list = []  # contains each dataframe from metric items
            for i in range(metric.num_metric_items):
                item = metric.metric_items[i]
                metric_items_df_list.append(
                    item.values.set_index("timestamp").add_suffix(f"-{i}")
                )
                kpi_map_list.append(item.metadata)
            df_kpi_map = pd.DataFrame(kpi_map_list)
            df_kpi = pd.concat(metric_items_df_list, axis=1)
            aggregate_metric(
                metric_name, metric_index, agg_exp_path, df_kpi_map, df_kpi
            )


def aggregate_directly(metric_path: str, aggregate_output_path: str):
    if not os.path.exists(aggregate_output_path):
        os.mkdir(aggregate_output_path)
    df_metric_names = pd.read_csv(TARGET_METRIC_NAMES_PATH)
    metric_names = df_metric_names["name"]
    num_metrics = len(metric_names)
    for metric_name in metric_names:
        metric_index = df_metric_names[df_metric_names["name"] == metric_name].index[0]
        print(f"Processing {metric_index+1}/{num_metrics} {metric_name} ...")
        metric_path = os.path.join(metric_path, f"metric-{metric_index}-day-1.json")
        with open(metric_path) as fp:
            metric_data = json.load(fp)
        metric = Metric(metric_name, metric_data)
        kpi_map_list = []  # contains each metadata from metric items
        metric_items_df_list = []  # contains each dataframe from metric items
        for i in range(metric.num_metric_items):
            item = metric.metric_items[i]
            metric_items_df_list.append(
                item.values.set_index("timestamp").add_suffix(f"-{i}")
            )
            kpi_map_list.append(item.metadata)
        df_kpi_map = pd.DataFrame(kpi_map_list)
        df_kpi = pd.concat(metric_items_df_list, axis=1)
        aggregate_metric(
            metric_name, metric_index, aggregate_output_path, df_kpi_map, df_kpi
        )


def count_kpis():
    df_metric_names = pd.read_csv(TARGET_METRIC_NAMES_PATH)
    metric_names = df_metric_names["name"]
    exp_names = get_exp_names()
    num_metrics = len(metric_names)
    num_kpis = 0
    for metric_name in metric_names:
        metric_index = df_metric_names[df_metric_names["name"] == metric_name].index[0]
        print(f"Processing {metric_index+1}/{num_metrics} {metric_name} ...")
        for exp_name in exp_names:
            metric = get_metric(exp_name, metric_name)
            kpi_map_list = []  # contains each metadata from metric items
            for i in range(metric.num_metric_items):
                item = metric.metric_items[i]
                kpi_map_list.append(item.metadata)
            df_kpi_map = pd.DataFrame(kpi_map_list)
            num_kpis += len(df_kpi_map)
    print(num_kpis)


def main():
    # aggregate_by_day()
    count_kpis()


if __name__ == "__main__":
    main()
