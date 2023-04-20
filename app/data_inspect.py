# Inspect number of KPIs
import pandas as pd
import os
import json
import jsonlines


def inspect_prometheus():
    metric_path = (
        "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/normal"
    )
    common_metrics = pd.read_csv(os.path.join(metric_path, "common_metrics.csv"))[
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
    print(f"{num_column} x {num_row}")


def main():
    inspect_prometheus()


if __name__ == "__main__":
    main()
