import os
import sys
import time
from concurrent import futures

import grpc
import pandas as pd
import predictor_service_pb2
import predictor_service_pb2_grpc
import predictor_utils
import predictors
import yaml
from logger import log

sys.path.append('.')

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

# Read configuration file
config_path = os.environ['CONFIG_PATH']
if config_path is not None:
    with open(config_path, 'r') as f:
        config = yaml.load(f)
else:
    log.fatal('Unable to read configuration file {}'.format(config_path))
    sys.exit(1)

# Create autoscaler dataset path name
AUTOSCALER_DATASET_PATH = os.path.join(
    config['bucketMountPath'],
    'autoscaler-dataset',
    predictor_utils.random_string(prefix='obi-autoscaler', suffix='.csv')
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
        job_type = predictor_utils.infer_predictor_name(req)
        if job_type is None:
            return predictor_service_pb2.PredictionResponse(
                Duration=-1,
                FailureProbability=.0
            )
        predictor = predictors.get_predictor_instance(job_type)
        # Get job arguments
        backend, day_diff = (None,) * 2
        args = req.JobArgs.split()
        for i, a in enumerate(args):
            if i + 1 >= len(args):
                break
            if a == '-s':
                # Next one is backend
                backend = args[i + 1]
            elif a == '-d':
                # Next one is day difference
                day_diff = args[i + 1]
        # Generate predictions
        predictions = predictor.predict(req.Metrics,
                                        backend=backend,
                                        day_diff=day_diff)
        # Return predictions to the user
        res = predictor_service_pb2.PredictionResponse()
        res.Duration = int(predictions)
        res.FailureProbability = 0.0  # predictions[1]
        res.Label = job_type
        log.info('Generated predictions: {}'.format(res))
        return res

    def RequestAutoscaling(self, req, ctx):
        """
        Collect data sent from OBI to train smart autoscalers
        :param data:
        :param ctx:
        :return:
        """
        log.info('Received request for autoscaler: {}'.format(req))

        # Request autoscaler's prediction
        scaling_factor = predictors.predict_scaling_factor(
            req.Metrics, req.Performance)

        # Build response
        res = predictor_service_pb2.AutoscalerResponse()
        res.scalingFactor = scaling_factor
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
            data.Nodes, data.ScalingFactor,
            # Metrics before scaling
            data.MetricsBefore.AMResourceLimitMB,
            data.MetricsBefore.AMResourceLimitVCores,
            data.MetricsBefore.UsedAMResourceMB,
            data.MetricsBefore.UsedAMResourceVCores,
            data.MetricsBefore.AppsSubmitted, data.MetricsBefore.AppsRunning,
            data.MetricsBefore.AppsPending, data.MetricsBefore.AppsCompleted,
            data.MetricsBefore.AppsKilled, data.MetricsBefore.AppsFailed,
            data.MetricsBefore.AggregateContainersPreempted,
            data.MetricsBefore.ActiveApplications,
            data.MetricsBefore.AppAttemptFirstContainerAllocationDelayNumOps,
            data.MetricsBefore.AppAttemptFirstContainerAllocationDelayAvgTime,
            data.MetricsBefore.AllocatedMB, data.MetricsBefore.AllocatedVCores,
            data.MetricsBefore.AllocatedContainers,
            data.MetricsBefore.AggregateContainersAllocated,
            data.MetricsBefore.AggregateContainersReleased,
            data.MetricsBefore.AvailableMB,
            data.MetricsBefore.AvailableVCores, data.MetricsBefore.PendingMB,
            data.MetricsBefore.PendingVCores,
            data.MetricsBefore.PendingContainers,
            # Metrics after scaling
            data.MetricsAfter.AMResourceLimitMB,
            data.MetricsAfter.AMResourceLimitVCores,
            data.MetricsAfter.UsedAMResourceMB,
            data.MetricsAfter.UsedAMResourceVCores,
            data.MetricsAfter.AppsSubmitted, data.MetricsAfter.AppsRunning,
            data.MetricsAfter.AppsPending, data.MetricsAfter.AppsCompleted,
            data.MetricsAfter.AppsKilled, data.MetricsAfter.AppsFailed,
            data.MetricsAfter.AggregateContainersPreempted,
            data.MetricsAfter.ActiveApplications,
            data.MetricsAfter.AppAttemptFirstContainerAllocationDelayNumOps,
            data.MetricsAfter.AppAttemptFirstContainerAllocationDelayAvgTime,
            data.MetricsAfter.AllocatedMB, data.MetricsAfter.AllocatedVCores,
            data.MetricsAfter.AllocatedContainers,
            data.MetricsAfter.AggregateContainersAllocated,
            data.MetricsAfter.AggregateContainersReleased,
            data.MetricsAfter.AvailableMB,
            data.MetricsAfter.AvailableVCores, data.MetricsAfter.PendingMB,
            data.MetricsAfter.PendingVCores,
            data.MetricsAfter.PendingContainers,
            # Performance metric before and after
            data.PerformanceBefore, data.PerformanceAfter
        ]

        # Persist the received data
        df = pd.DataFrame([point],
                          columns=predictor_utils.autoscaler_dataset_header)
        if os.path.exists(AUTOSCALER_DATASET_PATH):
            with open(AUTOSCALER_DATASET_PATH, 'a') as ds:
                df.to_csv(ds, header=False, index=False)
        else:
            with open(AUTOSCALER_DATASET_PATH, 'w') as ds:
                df.to_csv(ds, index=False)

        return predictor_service_pb2.EmptyResponse()


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
