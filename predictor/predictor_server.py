import predictor_service_pb2
import predictor_service_pb2_grpc

import grpc

from concurrent import futures
import os
import time

import yaml
from logger import log

import predictors
import predictor_utils

import pandas as pd

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

# Read configuration file
config_path = os.environ['CONFIG_PATH']
if config_path is not None:
    with open(config_path, 'r') as f:
        config = yaml.load(f)
else:
    log.fatal('Unable to read configuration file {}'.format(config_path))

# Create autoscaler dataset path name
AUTOSCALER_DATASET_PATH = os.path.join(
    [
        config['bucketMountPath'],
        'autoscaler-dataset',
        predictor_utils.random_string(prefix='obi-autoscaler')
    ]
)


class PredictorServer(predictor_service_pb2_grpc.ObiPredictorServicer):
    """
    This class implement the server side mechanisms for providing predictive
    feature to OBI through remote procedure call
    """

    def RequestPrediction(self, req, ctx):
        """
        Request prediction service
        :param req:
        :param ctx:
        :return:
        """
        log.info('Received request {}'.format(req))

        # Select the correct predictor
        predictor_name = predictor_utils.infer_predictor_name(req)
        predictor = predictors.get_predictor_instance(predictor_name)
        # Generate predictions
        predictions = predictor.predict(req.Metrics)
        # Return predictions to the user
        res = predictor_service_pb2.PredictionResponse()
        res.Duration = predictions[0]
        res.FailureProbability = predictions[1]
        log.info('Generated predictions: {}'.format(res))
        return res

    def CollectAutoscalerData(self, data, ctx):
        """
        Collect data sent from OBI to train smart autoscalers
        :param data:
        :param ctx:
        :return:
        """
        log.info('Received train point for autoscaler: {}'.format(data))

        # Build a list to be appended to a CSV file
        point = [
            data.Node, data.ScalingFatctor,
            # Metrics before scaling
            data.MetricsBefore.AMResourceLimitMB, data.MetricsBefore.AMResourceLimitVCores,
            data.MetricsBefore.UsedAMResourceMB, data.MetricsBefore.UsedAMResourceVCores,
            data.MetricsBefore.AppsSubmitted, data.MetricsBefore.AppsRunning,
            data.MetricsBefore.AppsPending, data.MetricsBefore.AppsCompleted,
            data.MetricsBefore.AppsKilled, data.MetricsBefore.AppsFailed,
            data.MetricsBefore.AggregateContainersPreempted, data.MetricsBefore.ActiveApplications,
            data.MetricsBefore.AppAttemptFirstContainerAllocationDelayNumOps,
            data.MetricsBefore.AppAttemptFirstContainerAllocationDelayAvgTime,
            data.MetricsBefore.AllocatedMB, data.MetricsBefore.AllocatedVCores,
            data.MetricsBefore.AllocatedContainers, data.MetricsBefore.AggregateContainersAllocated,
            data.MetricsBefore.AggregateContainersReleased, data.MetricsBefore.AvailableMB,
            data.MetricsBefore.AvailableVCores, data.MetricsBefore.PendingMB,
            data.MetricsBefore.PendingVCores, data.MetricsBefore.PendingContainers,
            # Metrics after scaling
            data.MetricsAfter.AMResourceLimitMB, data.MetricsAfter.AMResourceLimitVCores,
            data.MetricsAfter.UsedAMResourceMB, data.MetricsAfter.UsedAMResourceVCores,
            data.MetricsAfter.AppsSubmitted, data.MetricsAfter.AppsRunning,
            data.MetricsAfter.AppsPending, data.MetricsAfter.AppsCompleted,
            data.MetricsAfter.AppsKilled, data.MetricsAfter.AppsFailed,
            data.MetricsAfter.AggregateContainersPreempted, data.MetricsAfter.ActiveApplications,
            data.MetricsAfter.AppAttemptFirstContainerAllocationDelayNumOps,
            data.MetricsAfter.AppAttemptFirstContainerAllocationDelayAvgTime,
            data.MetricsAfter.AllocatedMB, data.MetricsAfter.AllocatedVCores,
            data.MetricsAfter.AllocatedContainers, data.MetricsAfter.AggregateContainersAllocated,
            data.MetricsAfter.AggregateContainersReleased, data.MetricsAfter.AvailableMB,
            data.MetricsAfter.AvailableVCores, data.MetricsAfter.PendingMB,
            data.MetricsAfter.PendingVCores, data.MetricsAfter.PendingContainers,
            # Performance metric before and after
            data.PerformanceBefore, data.PerformanceAfter
        ]

        # Persist the received data
        df = pd.DataFrame(point, columns=predictor_utils.autoscaler_dataset_header)
        with open(AUTOSCALER_DATASET_PATH, 'a') as ds:
            df.to_csv(ds)

        return None


def serve():
    """
    Instantiate and keep the server alive
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    predictor_service_pb2_grpc.add_ObiPredictorServicer_to_server(
        PredictorServer(), server)
    host = os.environ['SERVICE_HOST']
    port = int(os.environ['SERVICE_PORT'])
    log.info('Serving on {}:{}'.format(host, port))
    server.add_insecure_port('{}:{}'.format(host, port))
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    log.info('Starting gRPC server')
    serve()
