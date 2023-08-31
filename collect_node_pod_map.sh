#!/bin/sh

for i in {1..150}
do
    echo "i = $i"
    timestamp=$(date +%s)
    kubectl get pods -n alms -o wide > "node_maps/${timestamp}"
    sleep 60
done