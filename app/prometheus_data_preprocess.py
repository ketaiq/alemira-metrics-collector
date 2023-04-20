from functools import reduce
import json
import logging
import os

import pandas as pd

METRIC_PATH = "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/normal"
COMBINED_METRIC_PATH = os.path.join(METRIC_PATH, "combined")


class Metric:
    def __init__(self, name: str, data):
        self.metric_name = name
        self.metric_items = []
        if self.check_data(data):
            if type(data) is dict:
                for item in data["result"]:
                    if item["values"]:
                        self.metric_items.append(MetricItem(item))
            elif type(data) is list:
                self.metric_items = data
        self.num_metric_items = len(self.metric_items)

    def check_data(self, data) -> bool:
        if not data:
            logging.warning(f"Empty data in {self.metric_name}!")
            return False
        if type(data) is dict:
            if not data["result"]:
                logging.warning(f"Empty results in {self.metric_name}!")
                return False
            elif data["resultType"] != "matrix":
                logging.warning(
                    f"The format of input data is not supported in {self.metric_name}!"
                )
                return False
            else:
                return True
        elif type(data) is list:
            for item in data:
                if type(item) is not MetricItem:
                    logging.warning(
                        f"The type of {item} is not supported in metric collection!"
                    )
                    return False
            return True
        else:
            logging.warning(f"The type of {data} is not supported in metric data!")
            return False


class MetricItem:
    def __init__(self, result_item: dict):
        self.metadata = result_item["metric"]
        self.values = pd.DataFrame(
            result_item["values"], columns=["timestamp", "value"]
        ).astype({"timestamp": "int64", "value": "float64"})
        self.values["timestamp"] = pd.to_datetime(self.values["timestamp"], unit="s")


def get_metric_names(exp_name: str) -> pd.Series:
    metric_names_map_path = os.path.join(METRIC_PATH, exp_name, "metric_names_map.json")
    return pd.read_json(metric_names_map_path, orient="index", typ="series")


def get_exp_names() -> list:
    return list(
        filter(
            lambda f: os.path.isdir(os.path.join(METRIC_PATH, f))
            and f.startswith("day-"),
            os.listdir(METRIC_PATH),
        )
    )


def get_metric_index(exp_name: str, metric_name: str):
    metric_names_map_path = os.path.join(
        METRIC_PATH, exp_name, "metrics", "metric_names_map.json"
    )
    with open(metric_names_map_path) as fp:
        metric_names_map = list(json.load(fp).values())
    return metric_names_map.index(metric_name) + 1


def get_metric(exp_name: str, metric_name: str) -> Metric:
    metric_index = get_metric_index(exp_name, metric_name)
    metric_path = os.path.join(
        METRIC_PATH, exp_name, "metrics", f"metric-{metric_index}-day-1.json"
    )
    metric_data = None
    try:
        with open(metric_path) as fp:
            metric_data = json.load(fp)
    except json.JSONDecodeError as e:
        logging.error(f"{metric_name} in {exp_name} cannot be decoded!")
    return Metric(metric_name, metric_data)


def metric_is_too_large(exp_names: list, metric_name: str) -> bool:
    for exp_name in exp_names:
        metric_index = get_metric_index(exp_name, metric_name)
        metric_path = os.path.join(
            METRIC_PATH, exp_name, "metrics", f"metric-{metric_index}-day-1.json"
        )
        filesize = os.path.getsize(metric_path)
        # consider size larger than 50MB as too large
        if filesize / 1024 / 1024 > 50:
            return True
    return False


def gen_unique_kpi_maps(exp_names: list, metric_name: str) -> list:
    unique_kpi_index = 1
    unique_kpi_json_map = dict()  # dict of json strings of kpi maps
    for exp_name in exp_names:
        metric = get_metric(exp_name, metric_name)
        for i in range(len(metric.metric_items)):
            item = metric.metric_items[i]
            unique_kpi_json = json.dumps(item.metadata, sort_keys=True)
            if unique_kpi_json in unique_kpi_json_map:
                unique_kpi_json_map[unique_kpi_json][exp_name] = i
            else:
                unique_kpi_json_map[unique_kpi_json] = {
                    "index": unique_kpi_index,
                    exp_name: i,
                    "kpi": item.metadata,
                }
                unique_kpi_index += 1
    return list(unique_kpi_json_map.values())


def is_constant_df(df: pd.DataFrame) -> bool:
    return ((df == df.loc[0]).all()).all()


def merge_time_series(unique_kpi_maps: list, metric_name: str):
    all_records = []
    for kpi_map in unique_kpi_maps:
        # get folders that share the same KPI
        exp_names = [key for key in kpi_map if key.startswith("day")]
        combined_kpi = []  # list of dataframes of a single KPI
        for exp_name in exp_names:
            kpi_index = kpi_map[exp_name]
            metric = get_metric(exp_name, metric_name)
            kpi = metric.metric_items[kpi_index].values
            combined_kpi.append(kpi)
        # merge time series of the same KPI
        combined_kpi_df = pd.concat(combined_kpi, ignore_index=True)
        if not combined_kpi_df.empty and not is_constant_df(
            combined_kpi_df.drop(columns="timestamp")
        ):
            kpi_map_index = kpi_map["index"]
            combined_kpi_df = (
                combined_kpi_df.set_index("timestamp")
                .add_prefix(f"kpi-{kpi_map_index}-")
                .reset_index()
            )
            all_records += combined_kpi_df.to_dict("records")
    all_df = pd.DataFrame(all_records)
    if not all_df.empty:
        all_df = all_df.groupby("timestamp").sum().reset_index()
    return all_df


def merge_valid_prometheus_metrics():
    common_metric_names = pd.read_csv(os.path.join(METRIC_PATH, "common_metrics.csv"))[
        "name"
    ]
    with open(os.path.join(METRIC_PATH, "merged_prometheus_metric_indices.json")) as fp:
        metric_indices = json.load(fp)
    metric_indices = [int(i) for i in metric_indices]
    metric_indices.sort()
    exp_names = get_exp_names()
    num_metric_names = len(common_metric_names)
    if not os.path.exists(COMBINED_METRIC_PATH):
        os.mkdir(COMBINED_METRIC_PATH)

    for i in metric_indices:
        metric_name = common_metric_names[i]
        print(f"[{i}/{num_metric_names}] Processing {metric_name} ...")
        if metric_name.startswith("apiserver"):
            logging.warning(f"Ignore {metric_name}!")
            continue
        unique_kpi_maps = gen_unique_kpi_maps(exp_names, metric_name)
        # remove KPI with namespace different from alms
        useless_kpis = []
        for kpi_map in unique_kpi_maps:
            if "namespace" in kpi_map["kpi"] and kpi_map["kpi"]["namespace"] != "alms":
                useless_kpis.append(kpi_map)
        for kpi_map in useless_kpis:
            unique_kpi_maps.remove(kpi_map)
        # merge time series
        merged_df = merge_time_series(unique_kpi_maps, metric_name)
        # clean experment names in KPI map
        for kpi_map in unique_kpi_maps:
            useless_keys = [key for key in kpi_map if key.startswith("day-")]
            for key in useless_keys:
                kpi_map.pop(key)
        # consider non-empty features
        if not merged_df.empty:
            with open(
                os.path.join(COMBINED_METRIC_PATH, f"metric-{i}-kpi-map.json"),
                "w",
            ) as fp:
                json.dump(unique_kpi_maps, fp)
            merged_df.to_csv(
                os.path.join(COMBINED_METRIC_PATH, f"metric-{i}.csv"),
                index=False,
            )
            num_added_kpis = len(merged_df.columns) - 1
            print(f"Extract {num_added_kpis} KPIs for metric {metric_name}")


def main():
    logging.basicConfig(
        filename="prometheus_data_preprocess.log",
        level=logging.WARNING,
        format="%(levelname)s %(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
    )
    merge_valid_prometheus_metrics()


if __name__ == "__main__":
    main()
