import base64
import functions_framework
import subprocess
import calendar
import datetime
import os


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    if not os.path.isdir("node_maps"):
        os.mkdir("node_maps")
        print("Create node_maps folder")
    print(os.listdir("node_maps"))
    date = datetime.datetime.utcnow()
    timestamp = calendar.timegm(date.utctimetuple())
    subprocess.Popen(
        f'kubectl get pods -n alms -o wide > "node_maps/{timestamp}"', shell=True
    )
    # Print out the data from Pub/Sub, to prove that it worked
    print(base64.b64decode(cloud_event.data["message"]["data"]))
