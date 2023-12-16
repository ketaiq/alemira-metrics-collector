# alemira-metrics-collector
Collect system metrics from prometheus of alemira.

## Environment

Change Project Name

Authenticate
```sh
gcloud auth application-default login
```


```sh
conda create -n alemira -c conda-forge python=3.10 black requests locust pyyaml pandas scikit-learn matplotlib jsonlines
pip install google-cloud-monitoring

# collect node pod maps

nohup bash collect_node_pod_map.sh 150 &
tar -czvf archive.tar.gz node_maps
gcloud compute scp --project "peerless-column-365315" --zone "europe-west6-a" node-mapper-collector:/home/ketai/archive.tar.gz ~/Downloads/
```