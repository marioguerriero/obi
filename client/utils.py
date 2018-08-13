import master_rpc_service_pb2

def map_job_type(job_type):
    """
    Performs type mapping between the given job type (as a string)
    to the protobuf enum type
    :param job_type:
    :return:
    """
    if job_type == 'PySpark':
        return master_rpc_service_pb2.Job.PYSPARK
    return None
