# fetch APIs every 10s
import pandas as pd
from app import NORMAL_METRICS_PATH
from app.prometheus_apis import PrometheusAPI
import time, argparse, datetime, json, os


def collect_known_prometheus_metrics(
    username: str, password: str, start: str, end: str, output_path: str = ""
):
    """
    Collect metrics of last two weeks.

    Parameters
    ----------
    username : str
        username of target Prometheus server
    password : str
        password of target Prometheus server
    start : str
        start time of the experiment in ISO8601 format
    end : str
        end time of the experiment in ISO8601 format
    """
    experiment_start = datetime.datetime.fromisoformat(start)
    experiment_end = datetime.datetime.fromisoformat(end)
    prometheus_api = PrometheusAPI(username, password)
    metric_names = pd.read_csv(
        os.path.join(NORMAL_METRICS_PATH, "prometheus_target_metrics.csv")
    )["name"].to_list()

    step = "1m"
    metrics_dir = os.path.join(output_path, "prometheus-metrics")
    if not os.path.exists(metrics_dir):
        os.mkdir(metrics_dir)

    num_metric_names = len(metric_names)
    # store map of metric names
    metric_names_map = dict(
        [(index + 1, metric_name) for index, metric_name in enumerate(metric_names)]
    )
    with open(os.path.join(metrics_dir, "metric_names_map.json"), "w") as fp:
        json.dump(metric_names_map, fp, indent=4)
    # collect instant metric
    # total_metric_values = 0
    # for name in metric_names:
    #     instant_metric = prometheus_api.get_instant_metric(name)
    #     count = len(instant_metric["result"])
    #     print(f"{name}: {count}")
    #     total_metric_values += count
    # print(f"total metric values: {total_metric_values}")

    # collect range metric
    day_count = 1
    start = experiment_start
    while start < experiment_end:
        start_timestamp = int(start.timestamp())
        end = start + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
        if end > experiment_end:
            end = experiment_end
        end_timestamp = int(end.timestamp())

        for i in range(num_metric_names):
            name = metric_names[i]
            # the number after metric is the index of metric name in map of metric names
            metric_file = os.path.join(
                metrics_dir, f"metric-{i+1}-day-{day_count}.json"
            )
            print(
                f"collect metric {name} [{i+1}/{num_metric_names}] from day {day_count}"
            )
            range_metric = prometheus_api.get_range_metric(
                name, start_timestamp, end_timestamp, step
            )

            with open(metric_file, "w") as fp:
                json.dump(range_metric, fp, separators=(",", ":"))
            time.sleep(0.1)
        start += datetime.timedelta(days=1)
        day_count += 1


def collect_prometheus_metric_names(username: str, password: str, output_path: str):
    prometheus_api = PrometheusAPI(username, password)
    metric_names = prometheus_api.get_metric_names()
    df = pd.DataFrame({"name": metric_names})
    df.to_csv(os.path.join(output_path, "prometheus_metric_names.csv"), index=False)


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
    parser.add_argument("-s", "--start")
    parser.add_argument("-e", "--end")
    args = parser.parse_args()
    # check username and password
    if not args.username:
        args.username = input("Please enter prometheus username: ")
    if not args.password:
        args.password = input("Please enter prometheus password: ")
    collect_known_prometheus_metrics(args.username, args.password, args.start, args.end)


if __name__ == "__main__":
    main()
