# Inspect number of KPIs
import pandas as pd
import os
import json
import jsonlines


def inspect_prometheus():
    metric_path = (
        "/Users/ketai/Downloads/Alemira/Thesis/experiments/normal"
    )
    common_metrics = pd.read_csv(os.path.join(metric_path, "prometheus_target_metrics.csv"))[
        "name"
    ]
    prometheus_data_dirs = [
        dir for dir in os.listdir(metric_path) if dir.startswith("day")
    ]
    num_row = 60 * 24 * 14
    kpis = set()
    for prom_dir in prometheus_data_dirs:
        with open(
            os.path.join(metric_path, prom_dir, "metrics", "metric_names_map.json")
        ) as jsonfile:
            metric_names_map = list(json.load(jsonfile).values())
            for common_metric_name in common_metrics:
                metric_index = metric_names_map.index(common_metric_name) + 1
                submetric_path = os.path.join(
                    metric_path,
                    prom_dir,
                    "metrics",
                    f"metric-{metric_index}-day-1.json",
                )
                with open(submetric_path) as submetricfile:
                    submetric = json.load(submetricfile)
                for item in submetric["result"]:
                    if item["values"]:
                        kpis.add(json.dumps(item["metric"], sort_keys=True))
    num_column = len(kpis)
    print(f"Prometheus time series data shape: {num_row} x {num_column}")


def metric_type_exists(metric_path: str, folders: list, metric_type_index: int) -> bool:
    for folder in folders:
        if not os.path.exists(
            os.path.join(metric_path, folder, f"metric-type-{metric_type_index}")
        ):
            return False
    return True


def inspect_gcloud():
    experiments_path = "/Users/ketai/Downloads/Alemira/Thesis/experiments/normal"
    metric_path = os.path.join(experiments_path, "gcloud-metrics")
    folders = [
        folder
        for folder in os.listdir(metric_path)
        if folder.startswith("gcloud_metrics-day")
    ]
    metric_type_map_path = os.path.join(experiments_path, "gcloud_target_metrics.csv")
    metric_type_map = pd.read_csv(metric_type_map_path).set_index("index")
    num_column = 0
    num_row = 60 * 24 * 14
    for i in metric_type_map.index:
        if not metric_type_exists(metric_path, folders, i):
            continue
        kpis = set()
        for folder in folders:
            kpi_map_path = os.path.join(
                metric_path, folder, f"metric-type-{i}", "kpi_map.jsonl"
            )
            with jsonlines.open(kpi_map_path) as reader:
                for kpi_map in reader:
                    kpis.add(json.dumps(kpi_map["kpi"], sort_keys=True))
        num_column += len(kpis)
    print(f"GCloud time series data shape: {num_row} x {num_column}")


def main():
    inspect_prometheus()
    inspect_gcloud()


if __name__ == "__main__":
    main()
