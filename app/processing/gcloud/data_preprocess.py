import json
import os
import jsonlines

import pandas as pd


def has_same_metric_type():
    prev_metric_type = None
    folders = [
        folder
        for folder in os.listdir("gcloud-metrics")
        if folder.startswith("gcloud_metrics")
    ]
    for folder in folders:
        metric_type_map_path = os.path.join(
            "gcloud-metrics", folder, "metric_type_map.csv"
        )
        if not os.path.exists(metric_type_map_path):
            continue
        metric_type_map = pd.read_csv(metric_type_map_path)
        if prev_metric_type is None:
            prev_metric_type = metric_type_map
        elif not metric_type_map.equals(prev_metric_type):
            print(f"{folder} contains different metric type!")
            return False
    print(f"All metrics have the same type.")
    return True


def remove_duplicated_gcloud_metric_maps():
    folders = [
        folder
        for folder in os.listdir("gcloud-metrics")
        if folder.startswith("gcloud_metrics")
    ]
    for folder in folders:
        metric_type_map_path = os.path.join(
            "gcloud-metrics", folder, "metric_type_map.csv"
        )
        if os.path.exists(metric_type_map_path):
            os.remove(metric_type_map_path)


def gen_unique_kpi_maps(folders: list, metric_type_index: int) -> list:
    """
    Generate a list of unique KPI maps for each metric type in the format of
    {"index": <unique-int>, "gcloud_metrics-day-1": <kpi-index>, "gcloud_metrics-day-2": <kpi-index>, ..., "kpi": {...}}
    """
    unique_kpi_index = 1
    unique_kpi_json_map = dict()  # dict of json strings of kpi maps
    for folder in folders:
        kpi_map_path = os.path.join(
            "gcloud-metrics",
            folder,
            f"metric-type-{metric_type_index}",
            "kpi_map.jsonl",
        )
        with jsonlines.open(kpi_map_path) as reader:
            kpi_map = [obj for obj in reader]
        for kpi_map_item in kpi_map:
            unique_kpi_json = json.dumps(kpi_map_item["kpi"], sort_keys=True)
            if unique_kpi_json in unique_kpi_json_map:
                unique_kpi_json_map[unique_kpi_json][folder] = kpi_map_item["index"]
            else:
                unique_kpi_json_map[unique_kpi_json] = {
                    "index": unique_kpi_index,
                    folder: kpi_map_item["index"],
                    "kpi": kpi_map_item["kpi"],
                }
                unique_kpi_index += 1
    return list(unique_kpi_json_map.values())


def merge_time_series_across_days(
    unique_kpi_maps: list, metric_type_index: int
) -> pd.DataFrame:
    """Merge time series for each metric type and write to a csv file."""
    all_records = []
    useless_kpi_maps = []
    for kpi_map in unique_kpi_maps:
        # get folders that share the same KPI
        folders = [key for key in kpi_map if key.startswith("gcloud_metrics-day")]
        combined_kpi = []  # list of dataframes of a single KPI
        for folder in folders:
            kpi_index = kpi_map[folder]
            kpi_path = os.path.join(
                "gcloud-metrics",
                folder,
                f"metric-type-{metric_type_index}",
                f"kpi-{kpi_index}.csv",
            )
            combined_kpi.append(pd.read_csv(kpi_path))
        # merge time series of the same KPI
        combined_kpi_df = pd.concat(combined_kpi, ignore_index=True)
        if not combined_kpi_df.empty:
            kpi_map_index = kpi_map["index"]
            combined_kpi_df = (
                combined_kpi_df.set_index("timestamp")
                .add_prefix(f"kpi-{kpi_map_index}-")
                .reset_index()
            )
            all_records += combined_kpi_df.to_dict("records")
        else:
            useless_kpi_maps.append(kpi_map)
    # clean KPI map
    for kpi_map in useless_kpi_maps:
        unique_kpi_maps.remove(kpi_map)

    all_df = pd.DataFrame(all_records)
    if not all_df.empty:
        all_df = all_df.groupby("timestamp").sum().reset_index()

    return all_df


def merge_time_series_in_one_metric(
    kpi_map_list: list, metric_type_index: int, metrics_path: str
) -> pd.DataFrame:
    kpi_list = []
    for kpi_map in kpi_map_list:
        kpi_index = kpi_map["index"]
        kpi_path = os.path.join(
            metrics_path,
            f"metric-type-{metric_type_index}",
            f"kpi-{kpi_index}.csv",
        )
        df_kpi = pd.read_csv(kpi_path)
        df_kpi["timestamp"] = pd.to_datetime(df_kpi["timestamp"], unit="s").dt.round(
            "min"
        )
        df_kpi = df_kpi.set_index("timestamp").add_prefix(f"kpi-{kpi_index}-")
        kpi_list.append(df_kpi)
    df_kpis = pd.concat(kpi_list, axis=1)
    if len(df_kpis.index) != len(df_kpis.index.drop_duplicates()):
        df_kpis = df_kpis.groupby("timestamp").agg("mean")
    return df_kpis


def metric_type_exists(folders: list, metric_type_index: int) -> bool:
    for folder in folders:
        if not os.path.exists(
            os.path.join("gcloud-metrics", folder, f"metric-type-{metric_type_index}")
        ):
            return False
    return True


def merge_normal_gcloud_kpis_for_same_metric():
    """Merge non-constant and non-empty gcloud metrics in different days."""
    merge_destination = os.path.join("gcloud-metrics", "gcloud_combined")
    if not os.path.exists(merge_destination):
        os.mkdir(merge_destination)
    folders = [
        folder
        for folder in os.listdir("gcloud-metrics")
        if folder.startswith("gcloud_metrics")
    ]
    metric_type_map_path = os.path.join("gcloud-metrics", "metric_type_map.csv")
    metric_type_map = pd.read_csv(metric_type_map_path).set_index("index")
    max_metric_type_index = metric_type_map.index[-1]

    # for metric_type_index in metric_type_map.index:
    for metric_type_index in metric_type_map.index:
        metric_type = metric_type_map.loc[metric_type_index]
        # check metric type exist
        if not metric_type_exists(folders, metric_type_index):
            continue
        print(
            f"[{metric_type_index}/{max_metric_type_index}] Processing {metric_type} ..."
        )

        # create unique KPI maps
        unique_kpi_maps = gen_unique_kpi_maps(folders, metric_type_index)
        # merge time series
        merged_df = merge_time_series_across_days(unique_kpi_maps, metric_type_index)
        # remove useless keys in KPI maps
        for kpi_map in unique_kpi_maps:
            useless_keys = [
                key for key in kpi_map if key.startswith("gcloud_metrics-day-")
            ]
            for key in useless_keys:
                kpi_map.pop(key)

        if not merged_df.empty:
            with open(
                os.path.join(
                    merge_destination, f"metric-{metric_type_index}-kpi-map.json"
                ),
                "w",
            ) as fp:
                json.dump(unique_kpi_maps, fp)
            merged_df.to_csv(
                os.path.join(merge_destination, f"metric-{metric_type_index}.csv"),
                index=False,
            )


def copy_kpi_map_to_destination(
    folder: str, metrics_path: str, merge_destination: str
) -> list:
    kpi_map_path = os.path.join(
        metrics_path,
        folder,
        "kpi_map.jsonl",
    )
    with jsonlines.open(kpi_map_path) as reader:
        kpi_map_list = [obj for obj in reader]

    return kpi_map_list


def merge_faulty_gcloud_kpis_for_same_metric(metrics_parent_path: str):
    metrics_path = os.path.join(metrics_parent_path, "gcloud_metrics")
    merge_destination = os.path.join(metrics_parent_path, "gcloud_combined")
    if not os.path.exists(merge_destination):
        os.mkdir(merge_destination)
    metric_folders = [
        folder
        for folder in os.listdir(metrics_path)
        if folder.startswith("metric-type")
    ]
    for folder in metric_folders:
        metric_type_index = folder.lstrip("metric-type-")
        kpi_map_list = copy_kpi_map_to_destination(
            folder, metrics_path, merge_destination
        )
        df_kpis = merge_time_series_in_one_metric(
            kpi_map_list, metric_type_index, metrics_path
        )
        if df_kpis.empty:
            print(f"Metric {metric_type_index} doesn't contain KPIs!")
        with open(
            os.path.join(merge_destination, f"metric-{metric_type_index}-kpi-map.json"),
            "w",
        ) as fp:
            json.dump(kpi_map_list, fp)
        df_kpis.to_csv(
            os.path.join(merge_destination, f"metric-{metric_type_index}.csv"),
            index=False,
        )


def main():
    if has_same_metric_type():
        remove_duplicated_gcloud_metric_maps()
    merge_normal_gcloud_kpis_for_same_metric()


if __name__ == "__main__":
    main()
