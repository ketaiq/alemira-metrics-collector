# fetch APIs every 10s
from app.apis import PrometheusAPI
import time, argparse, datetime
import pandas as pd
import logging


def collect_metrics(username: str, password: str):
    """Collect metrics of last two weeks."""
    prometheus_api = PrometheusAPI(username, password)
    metric_names = prometheus_api.get_metric_names()
    total_metrics = len(metric_names)
    print(f"total metrics: {total_metrics}")

    experiment_begin = datetime.datetime.fromisoformat("2023-03-20T19:26:28.000+01:00")
    experiment_end = datetime.datetime.fromisoformat("2023-03-21T18:24:19.000+01:00")
    step = "1m"

    metric_names = prometheus_api.get_metric_names()
    num_metric_names = len(metric_names)
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
    start = experiment_begin
    while start < experiment_end:
        metrics = {}
        start_timestamp = int(start.timestamp())
        end = start + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
        if end > experiment_end:
            end = experiment_end
        end_timestamp = int(end.timestamp())

        for i in range(num_metric_names):
            name = metric_names[i]
            print(
                f"collect metric {name} [{i+1}/{num_metric_names}] from day {day_count}"
            )
            range_metric = prometheus_api.get_range_metric(
                name, start_timestamp, end_timestamp, step
            )
            if range_metric["result"]:
                metrics[name] = pd.Series(range_metric["result"])
            else:
                metrics[name] = pd.Series([])
            time.sleep(0.1)
        start += datetime.timedelta(days=1)
        df = pd.DataFrame(metrics)
        df.to_csv(f"day-{day_count}.csv", index=False)
        day_count += 1


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
    collect_metrics(args.username, args.password)


if __name__ == "__main__":
    main()
