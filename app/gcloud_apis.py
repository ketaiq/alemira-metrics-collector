# download metrics from google cloud monitoring
from google.cloud.monitoring_v3 import (
    MetricServiceClient,
    ListMetricDescriptorsRequest,
    ListMonitoredResourceDescriptorsRequest,
    ListTimeSeriesRequest,
)
from google.cloud.monitoring_v3.types import TimeInterval
from google.protobuf.timestamp_pb2 import Timestamp
from google.api.metric_pb2 import MetricDescriptor

from app.model.resource_label import ResourceLabel


class GCloudAPI:
    PROJECT_ID = "peerless-column-365315"
    KUBERNETES_METRIC_PREFIX = "kubernetes.io"
    KUBERNETES_METRIC_TYPES = ["autoscaler", "container", "node", "node_daemon", "pod"]
    GCLOUD_METRIC_PREFIX = "networking.googleapis.com"
    GCLOUD_METRIC_TYPES = [
        "node_flow",
        "pod_flow",
        "vpc_flow",
        "vm_flow",
    ]

    def __init__(self):
        self.client = MetricServiceClient()
        self.name = self.client.common_project_path(GCloudAPI.PROJECT_ID)

    def get_resource_labels(self, resource_type: str) -> list:
        resource_labels = []
        resource_descriptors_request = ListMonitoredResourceDescriptorsRequest(
            name=self.name, filter=f'resource.type = "{resource_type}"'
        )
        resource_descriptors_page_result = (
            self.client.list_monitored_resource_descriptors(
                resource_descriptors_request
            )
        )
        for resource_descriptor_response in resource_descriptors_page_result:
            resource_labels += [
                label.key for label in resource_descriptor_response.labels
            ]
        return resource_labels

    def get_time_series(
        self, metric_type: str, start: str, end: str, namespace: str = None
    ):
        """
        Get time series data of a specific metric type.

        Parameters
        ----------
        metric_type : str
            a complete string of the metric type
        start: str
            start time in ISO8601 format
        end: str
            end time in ISO8601 format
        namespace : str
            the namespace of the target services, default is None, which means to consider all namespaces
        """
        start_time = Timestamp()
        start_time.FromJsonString(start)
        end_time = Timestamp()
        end_time.FromJsonString(end)
        if namespace is not None:
            metric_filter = f'resource.labels.{ResourceLabel.NAMESPACE.value} = "{namespace}" AND metric.type = "{metric_type}"'
        else:
            metric_filter = f'metric.type = "{metric_type}"'
        time_series_request = ListTimeSeriesRequest(
            name=self.name,
            filter=metric_filter,
            interval=TimeInterval(start_time=start_time, end_time=end_time),
            view=ListTimeSeriesRequest.TimeSeriesView.FULL,
        )
        time_series_page_result = self.client.list_time_series(
            request=time_series_request
        )
        return time_series_page_result

    def get_metric_descriptors(self, metric_type_prefix: str):
        metric_descriptors_request = ListMetricDescriptorsRequest(
            name=self.name,
            filter=f'metric.type = starts_with("{metric_type_prefix}")',
        )
        metric_descriptors_page_result = self.client.list_metric_descriptors(
            request=metric_descriptors_request
        )
        return metric_descriptors_page_result

    @staticmethod
    def gen_all_metric_type_prefixes() -> list:
        """Generate all valid prefixes of metric types."""
        prefixes = []
        # all prefixes for Kubernetes metric types
        for mtype in GCloudAPI.KUBERNETES_METRIC_TYPES:
            prefixes.append(f"{GCloudAPI.KUBERNETES_METRIC_PREFIX}/{mtype}")
        # all prefixes for Google Cloud metric types
        for mtype in GCloudAPI.GCLOUD_METRIC_TYPES:
            prefixes.append(f"{GCloudAPI.GCLOUD_METRIC_PREFIX}/{mtype}")
        return prefixes
