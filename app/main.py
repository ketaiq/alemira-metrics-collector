# fetch APIs every 10s
from app.apis import PrometheusAPI
import time
import argparse


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
    if args.username is None:
        args.username = input("Please enter prometheus username: ")
    if args.password is None:
        args.password = input("Please enter prometheus password: ")
    prometheus_api = PrometheusAPI(username=args.username, password=args.password)
    while True:
        metric_names = prometheus_api.get_metric_names()
        for name in metric_names:
            metric = prometheus_api.get_metric_by_name(name)
            # TODO store to database
        time.sleep(10)


if __name__ == "__main__":
    main()
