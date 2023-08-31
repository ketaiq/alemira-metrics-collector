import os
import numpy as np
import plotly.express as px
import pandas as pd


def visualize_metrics_dynamics(df, title):
    df_ = df.copy()
    fig = px.line(
        df_,
        y=df_.columns,
        x=np.arange(len(df_.values)),
        title=title,
        width=2000,
        height=900,
    )
    fig.write_html(f"{title}.html")


def visualize_normal_metrics():
    metrics_path = "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/normal/complete_normal_time_series.csv"
    df_normal = pd.read_csv(metrics_path).set_index("timestamp")
    df_normal.index = pd.to_datetime(df_normal.index)
    df_normal.sort_index(inplace=True)
    visualize_metrics_dynamics(df_normal, "normal")


def visualize_faulty_metrics(metrics_path: str, experiment_name: str):
    df_faulty = pd.read_csv(metrics_path).set_index("timestamp")
    df_faulty.index = pd.to_datetime(df_faulty.index)
    visualize_metrics_dynamics(df_faulty, experiment_name)


def visualize_network_metrics(metrics_path: str, experiment_name: str):
    df_faulty = pd.read_csv(metrics_path).set_index("timestamp")
    df_faulty.index = pd.to_datetime(df_faulty.index)
    cols = [
        col
        for col in df_faulty.columns
        if "pm-11-alms-core-userapi" in col or "pm-12" in col
    ]
    visualize_metrics_dynamics(df_faulty[cols], "network-metrics-" + experiment_name)


def main():
    # visualize_normal_metrics()
    # failure_dir = "/Users/ketai/Library/CloudStorage/OneDrive-USI/Thesis/experiments/failure injection"
    # for folder in os.listdir(failure_dir):
    #     if (
    #         folder.startswith("day-")
    #         and "day-8-userapi-linear-network-delay-051308" in folder
    #     ):
    #         visualize_faulty_metrics(folder)
    experiment_name = "linear-network-delay-userhandlers-082316"
    visualize_faulty_metrics(
        f"/Users/ketai/Downloads/extra-experiments/{experiment_name}/{experiment_name}.csv",
        experiment_name,
    )


if __name__ == "__main__":
    main()
