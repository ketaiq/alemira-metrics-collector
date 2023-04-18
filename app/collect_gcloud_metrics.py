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


def collect_metrics(start: str, end: str):
    """
    Collect metrics of the experiment.

    Parameters
    ----------
    start : str
        start time of the experiment in ISO8601 format
    end : str
        end time of the experiment in ISO8601 format
    """
    metric_type_index = 1
    gcloud_api = GCloudAPI()
    metric_type_prefixes = gcloud_api.gen_all_metric_type_prefixes()
    for prefix in metric_type_prefixes:
        metric_descriptors = gcloud_api.get_metric_descriptors(prefix)
        for metric_desc in metric_descriptors:
            # store metric type
            GMetric.write_metric_type(metric_type_index, metric_desc.type)
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
                gmetric.write_kpi(metric_type_index, kpi_index)
                kpi_index += 1
            metric_type_index += 1
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser(
        prog="Google Cloud Metrics Collector for Alemira",
        description="Collect metrics from Google Cloud services",
    )
    parser.add_argument("-s", "--start")
    parser.add_argument("-e", "--end")
    args = parser.parse_args()
    collect_metrics(args.start, args.end)


if __name__ == "__main__":
    main()
