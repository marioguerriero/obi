import json
import socket
import urllib.request

from .hb_pb2 import *

HOSTNAME = socket.gethostname()
CLUSTER_NAME = '-'.join(HOSTNAME.split('-')[:-1])

QUERY = 'jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,' \
        'q1=default '
QUERY_URL = '{}:8088/{}'.format(HOSTNAME, QUERY)

RECEIVER_ADDRESS = ''
RECEIVER_PORT = 8080

# Create UDP socket object for sending heartbeat
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def send_hb():
    # Get Heartbeat message and serialized it
    hb = compute_hb()
    serialized = hb.SerializeToString()

    # Send heartbeat through UDP connection
    sock.sendto(serialized, (RECEIVER_ADDRESS, RECEIVER_PORT))


def compute_hb():
    # Initialize HB message
    hb = HeartbeatMessage()
    hb.cluster_name = CLUSTER_NAME

    # Collect metrics from Yarn master
    req = urllib.request.urlopen(QUERY_URL)
    req = req.read()
    metrics = json.loads(req)
    metrics = metrics['beans'][0]

    # Finish building heartbeat message
    hb.AMResourceLimitMB = metrics['AMResourceLimitMB']
    hb.AMResourceLimitVCores = metrics['AMResourceLimitVCores']
    hb.UsedAMResourceMB = metrics['UsedAMResourceMB']
    hb.UsedAMResourceVCores = metrics['UsedAMResourceVCores']
    hb.AppsSubmitted = metrics['AppsSubmitted']
    hb.AppsRunning = metrics['AppsRunning']
    hb.AppsPending = metrics['AppsPending']
    hb.AppsCompleted = metrics['AppsCompleted']
    hb.AppsKilled = metrics['AppsKilled']
    hb.AppsFailed = metrics['AppsFailed']
    hb.AggregateContainersPreempted = metrics['AggregateContainersPreempted']
    hb.ActiveApplications = metrics['ActiveApplications']
    hb.AppAttemptFirstContainerAllocationDelayNumOps = \
        metrics['AppAttemptFirstContainerAllocationDelayNumOps']
    hb.AppAttemptFirstContainerAllocationDelayAvgTime = \
        metrics['AppAttemptFirstContainerAllocationDelayAvgTime']
    hb.AllocatedMB = metrics['AllocatedMB']
    hb.AllocatedVCores = metrics['AllocatedVCores']
    hb.AllocatedContainers = metrics['AllocatedContainers']
    hb.AggregateContainersAllocated = metrics['AggregateContainersAllocated']
    hb.AggregateContainersReleased = metrics['AggregateContainersReleased']
    hb.AvailableMB = metrics['AvailableMB']
    hb.AvailableVCores = metrics['AvailableVCores']
    hb.PendingMB = metrics['PendingMB']
    hb.PendingVCores = metrics['PendingVCores']
    hb.PendingContainers = metrics['PendingContainers']

    return hb
