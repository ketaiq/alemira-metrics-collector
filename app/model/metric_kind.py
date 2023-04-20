from enum import Enum


class MetricKind(Enum):
    METRIC_KIND_UNSPECIFIED = 0
    GAUGE = 1
    DELTA = 2
    CUMULATIVE = 3
