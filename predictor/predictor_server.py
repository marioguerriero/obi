import predictor_service_pb2
import predictor_service_pb2_grpc

import grpc

from concurrent import futures
import os
import time

from logger import log

import predictors
import predictor_utils

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


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
        log.info(data)
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
