import socket

from .hb_pb2 import *

HOSTNAME = socket.gethostname()

CLUSTER_NAME = '-'.join(HOSTNAME.split('-')[:-1])


def send_hb():
    pass


def compute_msg():
    # Start building HB message
    hb = Heartbeat()
    hb.cluster_name = CLUSTER_NAME

    # Collect metrics from Yarn master

    return hb
