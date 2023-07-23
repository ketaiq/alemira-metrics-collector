from dataclasses import dataclass
from enum import Enum
import csv
import os

from app import FAILURE_INJECTION_PATH


class FailureType(Enum):
    CPUStress = "CPU Stress"
    MemoryStress = "Memory Stress"
    NetworkDelay = "Network Delay"
    NetworkLoss = "Network Loss"
    NetworkCorrupt = "Network Corrupt"


class FailurePattern(Enum):
    Constant = "Constant"
    Linear = "Linear"


class TargetService(Enum):
    userapi = "userapi"


@dataclass
class FailureInjectionLog:
    FAILURE_TYPE: FailureType
    FAILURE_PATTERN: FailurePattern
    SERVICE: TargetService
    experiment_begin_timestamp: str
    experiment_end_timestamp: str
    failure_begin_timestamp: str
    folder_name: str

    def to_dict(self):
        result = {}
        for k, v in self.__dict__.items():
            if isinstance(v, Enum):
                result[k] = v.value
            else:
                result[k] = v
        return result


class FailureInjectionLogHandler:
    def __init__(self, output_path: str):
        self.output_path = output_path

    def create_log_file(self, logs: list[FailureInjectionLog]):
        with open(self.output_path, "w", newline="") as csvfile:
            fieldnames = list(logs[0].__dict__.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for log in logs:
                writer.writerow(log.to_dict())

    def append_log_to_file(self, log: FailureInjectionLog):
        with open(self.output_path, "a", newline="") as csvfile:
            fieldnames = list(log.__dict__.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(log.to_dict())


def main():
    handler = FailureInjectionLogHandler(
        os.path.join(FAILURE_INJECTION_PATH, "failure-injection-logs.csv")
    )
    # log = FailureInjectionLog(
    #     FailureType.NetworkLoss,
    #     FailurePattern.Linear,
    #     TargetService.userapi,
    #     "2023-07-22T13:59:57+02:00",
    #     "2023-07-22T18:09:57+02:00",
    #     "2023-07-22T16:00:00+02:00",
    #     "linear-network-loss-userapi-072219",
    # )
    log = FailureInjectionLog(
        FailureType.NetworkLoss,
        FailurePattern.Constant,
        TargetService.userapi,
        "2023-05-15T12:30:44+02:00",
        "2023-05-15T15:01:00+02:00",
        "2023-05-15T13:01:00+02:00",
        "day-8-constant-network-loss-userapi-051512",
    )
    handler.append_log_to_file(log)


if __name__ == "__main__":
    main()
