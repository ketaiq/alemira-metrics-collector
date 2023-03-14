# fetch APIs every 10s
from app.apis import PrometheusAPI
import time, argparse, datetime

def collect_metrics(username: str, password: str):
    """Collect metrics of last two weeks."""
    prometheus_api = PrometheusAPI(username, password)
    metric_names = prometheus_api.get_metric_names()
    total_metrics = len(metric_names)
    print(f"total metrics: {total_metrics}")

    
    now = datetime.datetime.now()
    today_date = datetime.datetime(now.year, now.month, now.day)
    
    
    step = "1m"

    while True:
        metric_names = prometheus_api.get_metric_names()
        for name in metric_names:
            for day in range(14, 0, -1):
                instant_metric = prometheus_api.get_instant_metric(name)
                count = len(instant_metric["result"])
                print(f"{name}: {count}")

                start = int((today_date - datetime.timedelta(day)).timestamp())
                day -= 1
                end = int((today_date - datetime.timedelta(day)).timestamp() - 1)

                range_metric = prometheus_api.get_range_metric(name, start, end, step)
                count = len(range_metric["result"])
                print(f"{name}: {count}")
                time.sleep(1)


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
