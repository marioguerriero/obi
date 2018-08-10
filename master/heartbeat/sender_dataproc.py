import os
import json
import socket
import sys
import urllib.request

from .message_pb2 import HeartbeatMessage

HOSTNAME = socket.gethostname()
CLUSTER_NAME = HOSTNAME[:-2]

# Before doing anything, make sure that the current node is the master-old
GET_MASTER_CMD = '/usr/share/google/get_metadata_value attributes/dataproc-master-old'
master_name = os.popen(GET_MASTER_CMD).read()
if master_name != HOSTNAME:
    # If we are not in the master-old we should not send any heartbeat
    # so the current program can be aborted
    sys.exit(1)

QUERY = 'jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,' \
        'q1=default'
QUERY_URL = 'http://{}:8088/{}'.format(HOSTNAME, QUERY)

RECEIVER_ADDRESS = os.popen('/usr/share/google/'
                            'get_metadata_value attributes/'
                            'obi-hb-host').read()
RECEIVER_PORT = os.popen('/usr/share/google/'
                         'get_metadata_value attributes/'
                         'obi-hb-port').read()
RECEIVER_PORT = int(RECEIVER_PORT)

TIMEOUT = 10


def send_hb():
    # Get Heartbeat message and serialized it
    hb = compute_hb()
    serialized = hb.SerializeToString()

    # Create UDP socket object for sending heartbeat
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Send heartbeat through UDP connection
    sock.sendto(serialized, (RECEIVER_ADDRESS, RECEIVER_PORT))

    # Close socket connection
    sock.close()


def compute_hb():
    # Initialize HB message
    hb = HeartbeatMessage()
    hb.cluster_name = CLUSTER_NAME

    # Collect metrics from Yarn master-old
    req = urllib.request.urlopen(QUERY_URL)
    req = req.read().decode('utf-8')
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

    # Set service type to dataproc
    hb.ServiceType = 'dataproc'

    return hb


if __name__ == '__main__':
    send_hb()
