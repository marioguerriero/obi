import hashlib

import master_rpc_service_pb2


def map_job_type(job_type):
    """
    Performs type mapping between the given job type (as a string)
    to the protobuf enum type
    :param job_type:
    :return:
    """
    if job_type == 'PySpark':
        return master_rpc_service_pb2.JobSubmissionRequest.PYSPARK
    return None


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def executable_submission_iterator(path):
    CHUNK_SIZE = 65535
    with open(path, 'r') as f:
        data = f.read(CHUNK_SIZE)
        while data:
            yield master_rpc_service_pb2.ExecutableSubmissionRequest(
                filename=path,
                chunk=data
            )
            data = f.read(CHUNK_SIZE)
