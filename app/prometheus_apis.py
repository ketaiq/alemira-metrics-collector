# GET https://prometheus.crab.alemira.com/api/v1/label/__name__/values
# to get all names of metrics
import requests, os, time
from urllib3.exceptions import ConnectionError


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
            time.sleep(1)
        r.raise_for_status()
        return r.json()["data"]

    def get_instant_metric(self, name: str) -> dict:
        url = os.path.join(self.BASE_URL, "query")
        payload = {"query": name}
        for _ in range(3):
            r = requests.get(url, params=payload, auth=(self.username, self.password))
            if r.status_code < 300:
                break
            time.sleep(1)
        r.raise_for_status()
        json = r.json()
        if json["status"] == "success":
            return json["data"]
        else:
            print(f"Fail to get instant metric {name}. [JSON] {json}")
            return None

    def get_range_metric(self, name: str, start: str, end: str, step: str):
        """
        Get metric by name over a range of time.

        Parameters
        ----------
        name : the name of the collected metric
        start : start Unix timestamp in seconds, inclusive
        end : end Unix timestamp in seconds, inclusive
        step : query resolution step width in Prometheus duration string format
        """
        url = os.path.join(self.BASE_URL, "query_range")
        payload = {"query": name, "start": start, "end": end, "step": step}
        for _ in range(3):
            try:
                r = requests.get(
                    url, params=payload, auth=(self.username, self.password)
                )
                if r.status_code < 300:
                    break
                time.sleep(0.1)
            except ConnectionError as e:
                print(e.message)
                time.sleep(120)
                r = requests.get(
                    url, params=payload, auth=(self.username, self.password)
                )
        r.raise_for_status()
        json = r.json()
        if json["status"] == "success":
            return json["data"]
        else:
            print(f"Fail to get instant metric {name}. [JSON] {json}")
            return None
