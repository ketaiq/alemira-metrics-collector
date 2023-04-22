import pandas as pd
import os
import json
from dataclasses import dataclass


@dataclass
class GCloudAgg:
    NUM_KPI_MAPS_THRESHOLD = 16  # based on metric type 1
    GCLOUD_COMBINED_PATH = os.path.join("gcloud-metrics", "gcloud_combined")
    GCLOUD_ROUND_TIME_PATH = os.path.join("gcloud-metrics", "gcloud_round_time")
    AGG_OUTPUT_PATH = os.path.join("gcloud-metrics", "gcloud_aggregated")

    @staticmethod
    def get_metric_indices() -> list:
        return [
            filename.lstrip("metric-").rstrip("-kpi-map.json")
            for filename in os.listdir(GCloudAgg.GCLOUD_COMBINED_PATH)
            if filename.endswith("kpi-map.json")
        ]

    @staticmethod
    def get_df_kpi_map(metric_index: int) -> pd.DataFrame:
        kpi_map_path = os.path.join(
            GCloudAgg.GCLOUD_COMBINED_PATH, f"metric-{metric_index}-kpi-map.json"
        )
        with open(kpi_map_path) as fp:
            kpi_map_list = json.load(fp)
        kpi_maps = [kpi_map["kpi"] for kpi_map in kpi_map_list]
        indices = [kpi_map["index"] for kpi_map in kpi_map_list]
        df_kpi_map = pd.DataFrame(kpi_maps, index=indices)
        return df_kpi_map

    @staticmethod
    def get_df_metric(metric_index: int, is_round_time: bool = False) -> pd.DataFrame:
        if is_round_time:
            metric_path = os.path.join(
                GCloudAgg.GCLOUD_ROUND_TIME_PATH, f"metric-{metric_index}.csv"
            )
            df = pd.read_csv(metric_path)
            if "Unnamed: 0" in df.columns:
                df.drop(columns="Unnamed: 0", inplace=True)
            return df
        else:
            metric_path = os.path.join(
                GCloudAgg.GCLOUD_COMBINED_PATH, f"metric-{metric_index}.csv"
            )
            return pd.read_csv(metric_path)

    @staticmethod
    def index_list(series) -> list:
        return series.to_list()

    @staticmethod
    def is_distribution(cols: list) -> bool:
        for col in cols:
            if "count" in col or "mean" in col or "sum_of_squared_deviation" in col:
                return True
        return False

    @staticmethod
    def aggregate_with_container_name(metric_index: int, df_kpi_map: pd.DataFrame):
        """Aggregate KPIs with same container name."""
        print(f"Aggregating metric {metric_index} with same container name ...")
        # remove labels with same values
        isunique = df_kpi_map.nunique() == 1
        df_kpi_map_unique = df_kpi_map.drop(columns=isunique.index[isunique])
        # aggregate all columns except pod_name
        df_kpi_map_unique.drop(columns="pod_name", inplace=True)
        group_columns = df_kpi_map_unique.columns.to_list()
        df_kpi_map_unique = (
            df_kpi_map_unique.reset_index()
            .groupby(group_columns)
            .agg(GCloudAgg.index_list)
        )
        GCloudAgg.aggregate(metric_index, df_kpi_map_unique)

    @staticmethod
    def aggregate_with_all_kpis(metric_index: int, df_kpi_map: pd.DataFrame):
        """Aggregate KPIs with only different node names."""
        print(f"Aggregating metric {metric_index} with all KPIs ...")
        isunique = df_kpi_map.nunique() == 1
        df_same = df_kpi_map[isunique.index[isunique]]
        group_columns = df_same.columns.to_list()
        df_same = df_same.reset_index().groupby(group_columns).agg(GCloudAgg.index_list)
        GCloudAgg.aggregate(metric_index, df_same)

    @staticmethod
    def aggregate_with_pod_service(metric_index: int, df_kpi_map: pd.DataFrame):
        """Aggregate KPIs with same pod service extracted from the pod name."""
        print(f"Aggregating metric {metric_index} with same pod service ...")
        # remove labels with same values
        isunique = df_kpi_map.nunique() == 1
        df_kpi_map_unique = df_kpi_map.drop(columns=isunique.index[isunique])
        df_kpi_map_unique["pod_name"] = df_kpi_map_unique["pod_name"].str.extract(
            r"(alms[-a-z]+)-"
        )
        df_kpi_map_unique.drop(
            columns=[
                col
                for col in df_kpi_map_unique.columns
                if col not in ["pod_name", "remote_network"]
            ],
            inplace=True,
        )
        group_columns = ["pod_name"]
        if "remote_network" in df_kpi_map:
            group_columns.append("remote_network")
        df_kpi_map_unique = (
            df_kpi_map_unique.reset_index()
            .groupby(group_columns)
            .agg(GCloudAgg.index_list)
        )
        GCloudAgg.aggregate(metric_index, df_kpi_map_unique)

    @staticmethod
    def aggregate_with_selected_labels(metric_index: int, df_kpi_map: pd.DataFrame):
        """Aggregate KPIs with selected labels."""
        print(f"Aggregating metric {metric_index} with selected labels ...")
        group_columns = df_kpi_map.columns.to_list()
        df_kpi_map_unique = (
            df_kpi_map.reset_index().groupby(group_columns).agg(GCloudAgg.index_list)
        )
        GCloudAgg.aggregate(metric_index, df_kpi_map_unique)

    @staticmethod
    def adapt_no_agg(metric_index: int, df_kpi_map: pd.DataFrame):
        """Adapt metrics no need for aggregation."""
        print(f"Adapting metric {metric_index} without aggregation ...")
        df_metric = GCloudAgg.get_df_metric(metric_index, True).set_index("timestamp")
        new_kpi_map_list = []
        new_kpi_map_index = 1
        for i in df_kpi_map.index:
            original_column_name = f"kpi-{i}-value"
            if original_column_name in df_metric.columns:
                # adapt KPI map
                kpi_keys = df_kpi_map.columns
                kpi_values = df_kpi_map.loc[i]
                kpi = {kpi_keys[i]: kpi_values[i] for i in range(len(kpi_keys))}
                new_kpi_map = {"index": new_kpi_map_index, "kpi": kpi}
                new_kpi_map_list.append(new_kpi_map)
                # adapt metric
                df_metric.rename(
                    columns={original_column_name: f"agg-kpi-{new_kpi_map_index}"},
                    inplace=True,
                )
                new_kpi_map_index += 1
        with open(
            os.path.join(
                GCloudAgg.AGG_OUTPUT_PATH, f"metric-{metric_index}-kpi-map.json"
            ),
            "w",
        ) as fp:
            json.dump(new_kpi_map_list, fp)
        df_metric.to_csv(
            os.path.join(GCloudAgg.AGG_OUTPUT_PATH, f"metric-{metric_index}.csv")
        )

    @staticmethod
    def gen_df_metric_agg(df_metric_to_agg: pd.DataFrame) -> pd.DataFrame:
        min_series = df_metric_to_agg.min(axis=1).rename("min")
        max_series = df_metric_to_agg.max(axis=1).rename("max")
        mean_series = df_metric_to_agg.mean(axis=1).rename("mean")
        median_series = df_metric_to_agg.median(axis=1).rename("median")
        std_series = df_metric_to_agg.std(axis=1).rename("std")
        sum_series = df_metric_to_agg.sum(axis=1).rename("sum")
        quantile_series = df_metric_to_agg.quantile(
            [0.5, 0.75, 0.8, 0.9, 0.99], axis=1
        ).transpose()
        df_metric_agg = pd.concat(
            [
                min_series,
                max_series,
                mean_series,
                median_series,
                std_series,
                sum_series,
                quantile_series,
            ],
            axis=1,
        )
        return df_metric_agg

    @staticmethod
    def aggregate_normal(
        df_kpi_map_unique: pd.DataFrame, df_metric: pd.DataFrame, metric_index: int
    ):
        new_kpi_map_list = []
        new_kpi_map_index = 1
        df_agg_list = []
        for row in df_kpi_map_unique.itertuples():
            # generate new KPI
            kpi_keys = df_kpi_map_unique.index.names
            kpi_values = row[0]
            kpi = {kpi_keys[i]: kpi_values[i] for i in range(len(kpi_keys))}
            new_kpi_map = {"index": new_kpi_map_index, "kpi": kpi}

            indices_with_metrics = [
                int(column.lstrip("kpi-").rstrip("-value"))
                for column in df_metric.columns.to_list()
            ]
            valid_indices = list(set(row[1]) & set(indices_with_metrics))
            columns_to_merge = [f"kpi-{i}-value" for i in valid_indices]

            if len(columns_to_merge) == 1:
                new_kpi_map_list.append(new_kpi_map)
                single_column_name = columns_to_merge[0]
                df_agg_list.append(
                    df_metric[single_column_name].rename(f"agg-kpi-{new_kpi_map_index}")
                )
                new_kpi_map_index += 1
                continue
            elif len(columns_to_merge) == 0:
                continue
            new_kpi_map_list.append(new_kpi_map)
            # merge more than 1 columns
            df_metric_to_agg = df_metric[columns_to_merge]
            df_metric_agg = GCloudAgg.gen_df_metric_agg(df_metric_to_agg).add_prefix(
                f"agg-kpi-{new_kpi_map_index}-"
            )
            df_agg_list.append(df_metric_agg)
            new_kpi_map_index += 1
        df_complete_agg = pd.concat(df_agg_list, axis=1)
        if not df_complete_agg.empty:
            with open(
                os.path.join(
                    GCloudAgg.AGG_OUTPUT_PATH, f"metric-{metric_index}-kpi-map.json"
                ),
                "w",
            ) as fp:
                json.dump(new_kpi_map_list, fp)
            df_complete_agg.to_csv(
                os.path.join(GCloudAgg.AGG_OUTPUT_PATH, f"metric-{metric_index}.csv")
            )

    @staticmethod
    def aggregate_distribution(
        df_kpi_map_unique: pd.DataFrame, df_metric: pd.DataFrame, metric_index: int
    ):
        new_kpi_map_list = []
        new_kpi_map_index = 1
        df_agg_list = []
        for row in df_kpi_map_unique.itertuples():
            # generate new KPI
            kpi_keys = df_kpi_map_unique.index.names
            kpi_values = row[0]
            kpi = {kpi_keys[i]: kpi_values[i] for i in range(len(kpi_keys))}
            new_kpi_map = {"index": new_kpi_map_index, "kpi": kpi}

            indices_with_metrics = [
                int(column.split("-")[1]) for column in df_metric.columns.to_list()
            ]
            valid_indices = list(set(row[1]) & set(indices_with_metrics))
            columns_count_to_merge = [f"kpi-{i}-count" for i in valid_indices]
            columns_mean_to_merge = [f"kpi-{i}-mean" for i in valid_indices]
            columns_sd_to_merge = [
                f"kpi-{i}-sum_of_squared_deviation" for i in valid_indices
            ]

            if len(columns_count_to_merge) == 1:
                new_kpi_map_list.append(new_kpi_map)
                single_column_name = [
                    columns_count_to_merge[0],
                    columns_mean_to_merge[0],
                    columns_sd_to_merge[0],
                ]
                df_agg_list.append(
                    df_metric[single_column_name].rename(
                        columns={
                            columns_count_to_merge[
                                0
                            ]: f"agg-kpi-{new_kpi_map_index}-count",
                            columns_mean_to_merge[
                                0
                            ]: f"agg-kpi-{new_kpi_map_index}-mean",
                            columns_sd_to_merge[0]: f"agg-kpi-{new_kpi_map_index}-sd",
                        }
                    )
                )
                new_kpi_map_index += 1
                continue
            elif len(columns_count_to_merge) == 0:
                continue
            new_kpi_map_list.append(new_kpi_map)
            # merge more than 1 columns
            df_metric_count_to_agg = df_metric[columns_count_to_merge]
            df_metric_count_agg = (
                GCloudAgg.gen_df_metric_agg(df_metric_count_to_agg)
                .add_prefix(f"agg-kpi-{new_kpi_map_index}-")
                .add_suffix("-count")
            )
            df_agg_list.append(df_metric_count_agg)
            df_metric_mean_to_agg = df_metric[columns_mean_to_merge]
            df_metric_mean_agg = (
                GCloudAgg.gen_df_metric_agg(df_metric_mean_to_agg)
                .add_prefix(f"agg-kpi-{new_kpi_map_index}-")
                .add_suffix("-mean")
            )
            df_agg_list.append(df_metric_mean_agg)
            df_metric_sd_to_agg = df_metric[columns_sd_to_merge]
            df_metric_sd_agg = (
                GCloudAgg.gen_df_metric_agg(df_metric_sd_to_agg)
                .add_prefix(f"agg-kpi-{new_kpi_map_index}-")
                .add_suffix("-sd")
            )
            df_agg_list.append(df_metric_sd_agg)
            new_kpi_map_index += 1
        df_complete_agg = pd.concat(df_agg_list, axis=1)
        if not df_complete_agg.empty:
            with open(
                os.path.join(
                    GCloudAgg.AGG_OUTPUT_PATH, f"metric-{metric_index}-kpi-map.json"
                ),
                "w",
            ) as fp:
                json.dump(new_kpi_map_list, fp)
            df_complete_agg.to_csv(
                os.path.join(GCloudAgg.AGG_OUTPUT_PATH, f"metric-{metric_index}.csv")
            )

    @staticmethod
    def aggregate(metric_index: int, df_kpi_map_unique: pd.DataFrame):
        df_metric = GCloudAgg.get_df_metric(metric_index, True).set_index("timestamp")
        if GCloudAgg.is_distribution(df_metric.columns):
            GCloudAgg.aggregate_distribution(df_kpi_map_unique, df_metric, metric_index)
        else:
            GCloudAgg.aggregate_normal(df_kpi_map_unique, df_metric, metric_index)

    @staticmethod
    def perform_aggregation_for_all_metrics():
        if not os.path.exists(GCloudAgg.AGG_OUTPUT_PATH):
            os.mkdir(GCloudAgg.AGG_OUTPUT_PATH)
        for i in GCloudAgg.get_metric_indices():
            df_kpi_map = GCloudAgg.get_df_kpi_map(i)
            if len(df_kpi_map) > GCloudAgg.NUM_KPI_MAPS_THRESHOLD:
                if "pod_name" in df_kpi_map:
                    if "container_name" in df_kpi_map:
                        GCloudAgg.aggregate_with_container_name(i, df_kpi_map)
                    else:
                        GCloudAgg.aggregate_with_pod_service(i, df_kpi_map)
                else:
                    # ignore columns with only single one unique value
                    isunique = df_kpi_map.nunique() == 1
                    df_kpi_map_unique = df_kpi_map.drop(
                        columns=isunique.index[isunique]
                    )
                    # ignore columns with more than 20 unique values
                    isunique = df_kpi_map_unique.nunique() > 20
                    df_kpi_map_unique = df_kpi_map_unique.drop(
                        columns=isunique.index[isunique]
                    )
                    if len(df_kpi_map_unique.columns) <= 1:
                        GCloudAgg.aggregate_with_all_kpis(i, df_kpi_map)
                    else:
                        GCloudAgg.aggregate_with_selected_labels(i, df_kpi_map_unique)
            else:
                GCloudAgg.adapt_no_agg(i, df_kpi_map)

    @staticmethod
    def aggregate_by_minute():
        """Aggregate original metrics to minute granularity."""
        if not os.path.exists(GCloudAgg.GCLOUD_ROUND_TIME_PATH):
            os.mkdir(GCloudAgg.GCLOUD_ROUND_TIME_PATH)
        for metric_index in GCloudAgg.get_metric_indices():
            print(f"Processing metric {metric_index} ...")
            df_metric = GCloudAgg.get_df_metric(metric_index)
            df_metric["timestamp"] = pd.to_datetime(
                df_metric["timestamp"], unit="s"
            ).dt.round("min")
            if len(df_metric["timestamp"]) != len(
                df_metric["timestamp"].drop_duplicates()
            ):
                if GCloudAgg.is_distribution(df_metric.columns):
                    df_metric = df_metric.groupby("timestamp").agg(
                        "median"
                    )  # no condition match
                else:
                    df_metric = df_metric.groupby("timestamp").agg("mean")
            df_metric.reset_index().to_csv(
                os.path.join(
                    GCloudAgg.GCLOUD_ROUND_TIME_PATH, f"metric-{metric_index}.csv"
                ),
                index=False,
            )


def main():
    # GCloudAgg.aggregate_by_minute()
    GCloudAgg.perform_aggregation_for_all_metrics()


if __name__ == "__main__":
    main()
