import pandas as pd
import os


def find_pods_on_node(node_maps_path: str, node_name: str, timestamp: int) -> list[str]:
    """
    Find pods on the given node at the given timestamp.

    Parameters
    ----------
    node_maps_path : str
        complete path to the node maps
    node_name : str
        name of the node, e.g. "xkrg"
    timestamp : int
        UNIX timestamp
    """
    # read node map given the timestamp
    df_node_map = pd.read_fwf(os.path.join(node_maps_path, timestamp))
    df_node_map = df_node_map[["NAME", "NODE"]]
    df_node_map["NODE_ID"] = df_node_map["NODE"].str.slice(start=-4)
    df_node_map["POD_ID"] = df_node_map["NAME"].str.extract(r"alms-core-(.*)")
    return df_node_map[df_node_map["NODE_ID"] == node_name]["POD_ID"].to_list()


def main():
    node_name = "xrkg"
    timestamp = "1693212510"
    pods = find_pods_on_node("node_maps", node_name, timestamp)
    print(f"Find pods on the node {node_name} at timestamp {timestamp}:")
    for pod in pods:
        print(pod)


if __name__ == "__main__":
    main()
