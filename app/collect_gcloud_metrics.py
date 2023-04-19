import argparse
import logging
from app.gcloud_apis import GCloudAPI
from app.model.gmetric import GMetric
from app.model.resource_label import ResourceLabel
import time

TARGET_NAMESPACE = "alms"


def extract_resource_type(metric_desc):
    """Extract the single resource type from a metric descriptor response."""
    num_resource_types = len(metric_desc.monitored_resource_types)
    if num_resource_types == 0:
        logging.error("Resource type is not specified!")
    elif num_resource_types > 1:
        logging.warning(
            f"There are more than 1 resource types {metric_desc.monitored_resource_types}!"
        )
    else:
        return metric_desc.monitored_resource_types[0]


def collect_metrics(start: str, end: str, metrics_dir_suffix: str = ""):
    """
    Collect metrics of the experiment.

    Parameters
    ----------
    start : str
        start time of the experiment in ISO8601 format
    end : str
        end time of the experiment in ISO8601 format
    metrics_dir_suffix : str
        directory name suffix of metrics
    """
    metric_type_index = 1
    gcloud_api = GCloudAPI()
    metric_type_prefixes = gcloud_api.gen_all_metric_type_prefixes()
    for prefix in metric_type_prefixes:
        metric_descriptors = gcloud_api.get_metric_descriptors(prefix)
        for metric_desc in metric_descriptors:
            logging.info(f"Processing metric type {metric_desc.type} ...")
            if GMetric.metric_type_exists(metrics_dir_suffix, metric_desc.type):
                metric_type_index += 1
                logging.warning(f"Metric type {metric_desc.type} already exists!")
                continue
            # store metric type
            GMetric.write_metric_type(
                metrics_dir_suffix, metric_type_index, metric_desc.type
            )
            # collect time series
            resource_labels = gcloud_api.get_resource_labels(
                extract_resource_type(metric_desc)
            )
            if ResourceLabel.NAMESPACE.value in resource_labels:
                # consider only metrics from the target namespace
                time_series_pages = gcloud_api.get_time_series(
                    metric_desc.type, start, end, namespace=TARGET_NAMESPACE
                )
            else:
                time_series_pages = gcloud_api.get_time_series(
                    metric_desc.type, start, end
                )
            kpi_index = 1
            for time_series in time_series_pages:
                gmetric = GMetric.from_time_series(time_series)
                gmetric.write_kpi(metrics_dir_suffix, metric_type_index, kpi_index)
                kpi_index += 1
            metric_type_index += 1


def main():
    logging.basicConfig(
        filename="gcloud_metrics_collector.log",
        level=logging.WARNING,
        format="%(levelname)s %(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
    )
    # parser = argparse.ArgumentParser(
    #     prog="Google Cloud Metrics Collector for Alemira",
    #     description="Collect metrics from Google Cloud services",
    # )
    # parser.add_argument("-s", "--start")
    # parser.add_argument("-e", "--end")
    # args = parser.parse_args()
    # collect_metrics(args.start, args.end)

    times = [
        (
            "-day-1-linear-memory-identity",
            "2023-03-21T22:15:38.000+01:00",
            "2023-03-22T22:15:38.000+01:00",
        ),
        (
            "-day-1-linear-cpu-identity",
            "2023-03-23T10:33:07.000+01:00",
            "2023-03-24T10:33:07.000+01:00",
        ),
        (
            "-day-1-linear-memory-userhandlers",
            "2023-03-27T14:26:28.000+01:00",
            "2023-03-28T14:26:28.000+01:00",
        ),
        (
            "-day-1-linear-cpu-userhandlers",
            "2023-03-26T12:14:38.000+01:00",
            "2023-03-27T12:14:38.000+01:00",
        ),
        (
            "-day-8-linear-network-delay-userapi",
            "2023-04-18T07:35:24+02:00",
            "2023-04-18T12:35:24+02:00",
        ),
        (
            "-day-8-linear-network-delay-userapi-high",
            "2023-04-18T15:49:14+02:00",
            "2023-04-18T20:49:14+02:00",
        ),
    ]
    for i in range(len(times)):
        time = times[i]
        metrics_dir_suffix = time[0]
        start = time[1]
        end = time[2]
        collect_metrics(start, end, metrics_dir_suffix)


if __name__ == "__main__":
    main()
