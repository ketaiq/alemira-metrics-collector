import argparse
import os

import pandas as pd
from app import FAILURE_INJECTION_PATH

from app.collect_gcloud_metrics import (
    collect_all_gcloud_metrics,
    collect_known_gcloud_metrics,
)
from app.collect_prometheus_metrics import (
    collect_known_prometheus_metrics,
    collect_prometheus_metric_names,
)


def collect_metrics(
    username: str, password: str, start: str, end: str, output_path: str
):
    # collect GCloud time series
    # collect_known_gcloud_metrics(
    #     start,
    #     end,
    #     output_path,
    # )
    collect_all_gcloud_metrics(start, end, output_path)
    # collect Prometheus time series
    collect_known_prometheus_metrics(username, password, start, end, output_path)


def collect_metrics_for_all_faulty_experiments(username: str, password: str):
    df = pd.read_csv(
        os.path.join(
            FAILURE_INJECTION_PATH,
            "alemira failure injection log",
            "log-failure-injection.csv",
        )
    )
    for index, row in df.iterrows():
        start = row["Experiment Begin"]
        end = row["Experiment End"]
        exp_name = row["Folder Name"]
        output_path = os.path.join(FAILURE_INJECTION_PATH, exp_name)
        print(f"{index} Collect metrics for experiment {exp_name} ...")
        collect_metrics(username, password, start, end, output_path)


def main():
    parser = argparse.ArgumentParser(
        prog="Alemira Metrics Collector",
        description="Collect metrics from prometheus of alemira lms",
    )
    parser.add_argument(
        "-u",
        "--username",
    )
    parser.add_argument("-p", "--password")
    args = parser.parse_args()
    # check username and password
    if not args.username:
        args.username = input("Please enter prometheus username: ")
    if not args.password:
        args.password = input("Please enter prometheus password: ")

    start = "2023-12-16T08:25:37+01:00"
    end = "2023-12-16T10:55:37+01:00"
    folder = "linear-network-delay-userapi-121608"
    # output_path = os.path.join(FAILURE_INJECTION_PATH, folder)
    output_path = os.path.join("/Users/ketai/Downloads/extra-experiments", folder)
    collect_metrics(args.username, args.password, start, end, output_path)
    # collect_all_gcloud_metrics(start, end, output_path)
    # collect_metrics_for_all_faulty_experiments(args.username, args.password)
    # collect_prometheus_metric_names(args.username, args.password, output_path)


if __name__ == "__main__":
    main()
