import os
import pandas as pd
from app.collect_metrics import FAILURE_INJECTION_PATH
from app.processing.gcloud.data_aggregate import GCloudAgg
from app.processing.gcloud.data_merge import remove_constants_and_merge
from app.processing.gcloud.data_preprocess import (
    merge_faulty_gcloud_kpis_for_same_metric,
)
from app.processing.prometheus.data_aggregate import aggregate_directly
from app.processing.prometheus.data_merge import merge_individual_metric


def merge_faulty_metrics():
    df = pd.read_csv(
        os.path.join(FAILURE_INJECTION_PATH, "alemira_failure_injection_log.csv")
    )
    for index, row in df.iterrows():
        exp_path = os.path.join(FAILURE_INJECTION_PATH, row["Folder Name"])
        merge_faulty_gcloud_kpis_for_same_metric(exp_path)
        gcloud_agg = GCloudAgg(
            os.path.join(exp_path, "gcloud_combined"),
            os.path.join(exp_path, "gcloud_round_time"),
            os.path.join(exp_path, "gcloud_aggregated"),
        )
        gcloud_agg.aggregate_by_minute()
        gcloud_agg.perform_aggregation_for_all_metrics()
        remove_constants_and_merge(exp_path)
        merge_output_path = os.path.join(exp_path, "prometheus_merged")
        aggregate_output_path = os.path.join(exp_path, "prometheus_aggregated")
        aggregate_directly(
            os.path.join(exp_path, "prometheus-metrics"),
            aggregate_output_path,
        )
        merge_individual_metric(merge_output_path, aggregate_output_path)
        merge_complete_metrics(exp_path, merge_output_path)


def merge_complete_metrics(metric_path, merge_output_path):
    df_locust = pd.read_csv(
        os.path.join(metric_path, "locust_cleaned_agg_stats.csv")
    ).set_index("timestamp")
    df_gcloud = (
        pd.read_csv(os.path.join(metric_path, "gcloud-complete-time-series.csv"))
        .set_index("timestamp")
        .add_prefix("gc-")
    )
    df_prometheus = pd.read_csv(
        os.path.join(merge_output_path, "prom-complete-time-series.csv")
    ).set_index("timestamp")
    df = pd.concat([df_locust, df_gcloud, df_prometheus], axis=1)
    df = df.loc[df_locust.index]
    df.fillna(0, inplace=True)
    df.to_csv(os.path.join(metric_path, "complete-time-series.csv"))


def main():
    # merge_normal_metrics()
    merge_faulty_metrics()


if __name__ == "__main__":
    main()
