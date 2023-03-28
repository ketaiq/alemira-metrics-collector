# alemira-metrics-collector
Collect system metrics from prometheus of alemira.

## 1. Experiment Time

### 1.1 Normal Execution
| No. | Workload | Experiment Begin | Experiment End |
| --- | -------- | ---------------- | -------------- |
| 1 | Day 1 | 2023-03-15T18:57:20.000+01:00 | 2023-03-16T17:56:32.000+01:00 |

### 1.2 Failure Injection
| No. | Type | Pattern | Service | Workload | Experiment Begin | Experiment End | Failure Begin | Failure End |
| --- | ---- | ------- | ------- | -------- | ---------------- | -------------- | ------------- | ----------- |
| 1 | memory stress | linear | identity | Day 1 | 2023-03-21T22:15:38.000+01:00 | 2023-03-22T22:14:50.000+01:00 | 2023-03-22T07:25:00.000+01:00 | 2023-03-22T10:25:00.000+01:00 |
| 2 | cpu stress | linear | identity | Day 1 | 2023-03-23T10:33:07.000+01:00 | 2023-03-24T10:30:04.000+01:00 | 2023-03-23T20:44:00.000+01:00 | 2023-03-23T23:14:00.000+01:00 |
| 3 | memory stress | linear | userhandlers | Day 1 | 2023-03-27T14:26:28.000+01:00 | 2023-03-28T14:24:36.000+01:00 | 2023-03-27T23:36:28+01:00 | 2023-03-28T02:36:28+01:00 |
| 4 | cpu stress | linear | userhandlers | Day 1 | 2023-03-26T12:14:38.000+01:00 | 2023-03-27T12:13:39.000+01:00 | 2023-03-26T21:24:38+01:00 | 2023-03-27T00:24:38+01:00 |