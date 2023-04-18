# contains models for representing metrics from google cloud monitoring
from dataclasses import dataclass
import logging
import os
import pandas as pd
import csv
import jsonlines
from app.model.value_type import ValueType


@dataclass
class GMetric:
    METRICS_DIR = "gcloud_metrics"
    METRIC_TYPE_MAP_FNAME = "metric_type_map.csv"  # stores index -> metric type
    KPI_MAP_FNAME = "kpi_map.jsonl"  # each KPI is a unique combination of labels for a specifc metric type
    mtype: str
    labels: dict
    df_points: pd.DataFrame

    @classmethod
    def from_time_series(cls, time_series) -> "GMetric":
        mtype = time_series.metric.type
        labels = dict(time_series.metric.labels)
        df_points = None
        if time_series.value_type == ValueType.DOUBLE.value:
            points = [
                (int(point.interval.start_time.timestamp()), point.value.double_value)
                for point in time_series.points
            ]
            df_points = pd.DataFrame(points, columns=["timestamp", "value"])
        elif time_series.value_type == ValueType.INT64.value:
            points = [
                (int(point.interval.start_time.timestamp()), point.value.int64_value)
                for point in time_series.points
            ]
            df_points = pd.DataFrame(points, columns=["timestamp", "value"])
        elif time_series.value_type == ValueType.DISTRIBUTION.value:
            points = [
                (
                    int(point.interval.start_time.timestamp()),
                    point.value.distribution_value.count,
                    point.value.distribution_value.mean,
                    point.value.distribution_value.sum_of_squared_deviation,
                )
                for point in time_series.points
            ]
            df_points = pd.DataFrame(
                points,
                columns=["timestamp", "count", "mean", "sum_of_squared_deviation"],
            )
        elif time_series.value_type == ValueType.BOOL.value:
            points = [
                (int(point.interval.start_time.timestamp()), 1)
                if point.value.bool_value
                else (int(point.interval.start_time.timestamp()), 0)
                for point in time_series.points
            ]
            df_points = pd.DataFrame(points, columns=["timestamp", "value"])
        else:
            logging.warning(
                f"Unsupported type {time_series.value_type} for time series of metric {mtype}!"
            )
        return GMetric(mtype, labels, df_points)

    @staticmethod
    def write_metric_type(metric_type_index: int, metric_type: str):
        # store metric type map
        if not os.path.exists(GMetric.METRICS_DIR):
            os.mkdir(GMetric.METRICS_DIR)
        metric_type_map_path = os.path.join(
            GMetric.METRICS_DIR, GMetric.METRIC_TYPE_MAP_FNAME
        )
        fieldnames = ["index", "metric_type"]
        if not os.path.exists(metric_type_map_path):
            with open(metric_type_map_path, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames)
                writer.writeheader()
                writer.writerow(
                    {"index": metric_type_index, "metric_type": metric_type}
                )
        else:
            with open(metric_type_map_path, "a", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames)
                writer.writerow(
                    {"index": metric_type_index, "metric_type": metric_type}
                )

    def write_kpi(self, metric_type_index: int, kpi_index: int):
        # write KPI labels map
        kpi_dir = os.path.join(GMetric.METRICS_DIR, f"metric-type-{metric_type_index}")
        if not os.path.exists(kpi_dir):
            os.mkdir(kpi_dir)
        kpi_map_path = os.path.join(kpi_dir, GMetric.KPI_MAP_FNAME)
        with jsonlines.open(kpi_map_path, "a") as jsonlfile:
            jsonlfile.write({"index": kpi_index, "kpi": self.labels})
        # write KPI time series
        kpi_path = os.path.join(kpi_dir, f"kpi-{kpi_index}.csv")
        if self.df_points is not None and not self.df_points.empty:
            self.df_points.to_csv(kpi_path, index=False)

    @staticmethod
    def metric_type_exists(metric_type: str) -> bool:
        """Check if the given metric type already exists."""
        metric_type_map_path = os.path.join(
            GMetric.METRICS_DIR, GMetric.METRIC_TYPE_MAP_FNAME
        )
        fieldnames = ["index", "metric_type"]
        if os.path.exists(metric_type_map_path):
            with open(metric_type_map_path, newline="") as csvfile:
                reader = csv.DictReader(csvfile, fieldnames)
                for row in reader:
                    if row["metric_type"] == metric_type:
                        return True
        return False
