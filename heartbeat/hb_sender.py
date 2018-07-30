import socket

from .hb_pb2 import *

HOSTNAME = socket.gethostname()
CLUSTER_NAME = '-'.join(HOSTNAME.split('-')[:-1])

QUERY = 'jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=default'
QUERY_URL = '{}:8088/{}'.format(HOSTNAME, QUERY)

RECEIVER_ADDRESS = ''


def send_hb():
    pass


def compute_msg():
    # Initialize HB message
    hb = Heartbeat()
    hb.cluster_name = CLUSTER_NAME

    # Collect metrics from Yarn master

    # Finish building heartbeat message

    return hb
