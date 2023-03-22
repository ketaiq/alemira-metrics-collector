# alemira-metrics-collector
Collect system metrics from prometheus of alemira.

## 1. Experiment Time

### 1.1 Normal Execution
| Day | Begin | End |
| --- | ----- | --- |
| 1 | 2023-03-15T18:57:20.000+01:00 | 2023-03-16T17:56:32.000+01:00 |

### 1.2 Failure Injection
| Day | Type | Pattern | Experiment Begin | Experiment End | Failure Begin | Failure End |
| --- | ---- | ------- | ---------------- | -------------- | ------------- | ----------- |
| 1 | memory stress | linear | 2023-03-20T19:26:28.000+01:00 | 2023-03-21T18:24:19.000+01:00 | 2023-03-21T04:36:00.000+01:00 | 2023-03-21T07:36:00.000+01:00