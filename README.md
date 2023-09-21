# alemira-metrics-collector
Collect system metrics from prometheus of alemira.

## 1. Experiment Time

### 1.1 Normal Execution
| No. | Workload | Experiment Begin | Experiment End |
| --- | -------- | ---------------- | -------------- |
| 1 | Day 1 | 2023-03-28T14:54:05+02:00 | 2023-03-29T14:54:05+02:00 |
| 2 | Day 2 | 2023-03-29T17:15:40+02:00 | 2023-03-30T17:15:40+02:00 |
| 3 | Day 3 | 2023-03-30T19:39:54+02:00 | 2023-03-31T19:39:54+02:00 |
| 4 | Day 4 | 2023-03-31T19:56:58+02:00 | 2023-04-01T19:56:58+02:00 |
| 5 | Day 5 | 2023-04-01T20:47:32+02:00 | 2023-04-02T20:47:32+02:00 |
| 6 | Day 6 | 2023-04-02T21:48:03+02:00 | 2023-04-03T21:48:03+02:00 |
| 7 | Day 7 | 2023-04-04T08:18:25+02:00 | 2023-04-05T08:18:25+02:00 |
| 8 | Day 8 | 2023-04-05T08:53:00+02:00 | 2023-04-06T08:53:00+02:00 |
| 9 | Day 9 | 2023-04-06T10:13:34+02:00 | 2023-04-07T10:13:34+02:00 |
| 10 | Day 10 | 2023-04-08T19:01:09+02:00 | 2023-04-09T19:01:09+02:00 |
| 11 | Day 11 | 2023-04-09T20:11:40+02:00 | 2023-04-10T20:11:40+02:00 |
| 12 | Day 12 | 2023-04-10T20:27:13+02:00 | 2023-04-11T20:27:13+02:00 |
| 13 | Day 13 | 2023-04-11T21:25:23+02:00 | 2023-04-12T21:25:23+02:00 |
| 14 | Day 14 | 2023-04-12T22:00:52+02:00 | 2023-04-13T22:00:52+02:00 |

### 1.2 Failure Injection
| No. | Type | Pattern | Service | Workload | Experiment Begin | Experiment End | Failure Begin | Failure End |
| --- | ---- | ------- | ------- | -------- | ---------------- | -------------- | ------------- | ----------- |
| 1 | memory stress | linear | identity | Day 1 | 2023-03-21T22:15:38.000+01:00 | 2023-03-22T22:14:50.000+01:00 | 2023-03-22T07:25:00.000+01:00 | 2023-03-22T10:25:00.000+01:00 |
| 2 | cpu stress | linear | identity | Day 1 | 2023-03-23T10:33:07.000+01:00 | 2023-03-24T10:30:04.000+01:00 | 2023-03-23T20:44:00.000+01:00 | 2023-03-23T23:14:00.000+01:00 |
| 3 | memory stress | linear | userhandlers | Day 1 | 2023-03-27T14:26:28.000+01:00 | 2023-03-28T14:24:36.000+01:00 | 2023-03-27T23:36:28+01:00 | 2023-03-28T02:36:28+01:00 |
| 4 | cpu stress | linear | userhandlers | Day 1 | 2023-03-26T12:14:38.000+01:00 | 2023-03-27T12:13:39.000+01:00 | 2023-03-26T21:24:38+01:00 | 2023-03-27T00:24:38+01:00 |
| 6 | network delay | linear | userapi | Day 8 | 2023-04-18T07:35:24+02:00 | 2023-04-18T12:35:24+02:00 | 2023-04-18T08:35:24+02:00 | 2023-04-18T11:35:24+02:00 |
| 7 | network delay high | linear | userapi | Day 8 | 2023-04-18T15:49:14+02:00 | 2023-04-18T20:49:14+02:00 | 2023-04-18T16:49:14+02:00 | 2023-04-18T19:49:14+02:00 |
| 8 | network loss | linear | userapi | Day 8 | 2023-04-20T20:23:11+02:00 | 2023-04-21T01:23:11+02:00 | 2023-04-20T21:25:00+02:00 | 2023-04-21T00:25:00+02:00 |
| 9 | network corrupt | linear | userapi | Day 8 | 2023-04-21T08:27:49+02:00 | 2023-04-21T13:27:49+02:00 | 2023-04-21T09:28:00+02:00 | 2023-04-21T12:28:00+02:00 |

## 2. Environment

```sh
conda create -n alemira -c conda-forge python=3.10 black requests locust pyyaml pandas scikit-learn matplotlib jsonlines
pip install google-cloud-monitoring

# collect node pod maps

nohup bash collect_node_pod_map.sh 150 &
tar -czvf archive.tar.gz node_maps
gcloud compute scp --project "peerless-column-365315" --zone "europe-west6-a" node-mapper-collector:/home/ketai/archive.tar.gz ~/Downloads/
```