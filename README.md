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
| 1 | memory stress | linear | 2023-03-21T22:15:38.000+01:00 | 2023-03-22T22:14:50.000+01:00 | 2023-03-22T07:25:00.000+01:00 | 2023-03-22T10:25:00.000+01:00