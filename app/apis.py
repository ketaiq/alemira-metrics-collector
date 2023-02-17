# GET https://prometheus.crab.alemira.com/api/v1/label/__name__/values
# to get all names of metrics
import requests
import os


class PrometheusAPI:
    BASE_URL = "https://prometheus.crab.alemira.com/api/v1"

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def get_metric_names(self) -> list:
        url = os.path.join(self.BASE_URL, "label", "__name__", "values")
        for _ in range(3):
            r = requests.get(url, auth=(self.username, self.password))
            if r.status_code < 300:
                break
        r.raise_for_status()
        return r.json()["data"]

    def get_metric_by_name(self, name: str) -> dict:
        url = os.path.join(self.BASE_URL, "query")
        payload = {"query": name}
        for _ in range(3):
            r = requests.get(url, params=payload, auth=(self.username, self.password))
            if r.status_code < 300:
                break
        r.raise_for_status()
        return r.json()["data"]
