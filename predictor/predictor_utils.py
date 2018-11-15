# Copyright 2018 
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import datetime
import os
import random
import string

from google.cloud import storage

predictor_matchers = {
    # TODO: do not use absolute paths in source_code fields
    'csv_find': {
        'file_names': ['find_changes.py', 'find_changes_test.py'],
        'source_code': ['/mnt/dhg-obi/cluster-script/find_changes_test.py']
    },
    'csv_update': {
        'file_names': ['update.py', 'update_test.py'],
        'source_code': ['/mnt/dhg-obi/cluster-script/update_test.py']
    },
    'csv_recreate': {
        'file_names': ['recreate_file.py'],
        'source_code': ['data/source/csv_recreate']
    },
    'ulm': {
        'file_names': ['ulm.py'],
        'source_code': ['data/source/ulm']
    },
}

autoscaler_dataset_header = [
    'Node', 'ScalingFatctor',
    # Metrics before 'scaling
    'MetricsBefore.AMResourceLimitMB', 'MetricsBefore.AMResourceLimitVCores',
    'MetricsBefore.UsedAMResourceMB', 'MetricsBefore.UsedAMResourceVCores',
    'MetricsBefore.AppsSubmitted', 'MetricsBefore.AppsRunning',
    'MetricsBefore.AppsPending', 'MetricsBefore.AppsCompleted',
    'MetricsBefore.AppsKilled', 'MetricsBefore.AppsFailed',
    'MetricsBefore.AggregateContainersPreempted',
    'MetricsBefore.ActiveApplications',
    'MetricsBefore.AppAttemptFirstContainerAllocationDelayNumOps',
    'MetricsBefore.AppAttemptFirstContainerAllocationDelayAvgTime',
    'MetricsBefore.AllocatedMB', 'MetricsBefore.AllocatedVCores',
    'MetricsBefore.AllocatedContainers',
    'MetricsBefore.AggregateContainersAllocated',
    'MetricsBefore.AggregateContainersReleased', 'MetricsBefore.AvailableMB',
    'MetricsBefore.AvailableVCores', 'MetricsBefore.PendingMB',
    'MetricsBefore.PendingVCores', 'MetricsBefore.PendingContainers',
    # Metrics after scaling
    'MetricsAfter.AMResourceLimitMB', 'MetricsAfter.AMResourceLimitVCores',
    'MetricsAfter.UsedAMResourceMB', 'MetricsAfter.UsedAMResourceVCores',
    'MetricsAfter.AppsSubmitted', 'MetricsAfter.AppsRunning',
    'MetricsAfter.AppsPending', 'MetricsAfter.AppsCompleted',
    'MetricsAfter.AppsKilled', 'MetricsAfter.AppsFailed',
    'MetricsAfter.AggregateContainersPreempted',
    'MetricsAfter.ActiveApplications',
    'MetricsAfter.AppAttemptFirstContainerAllocationDelayNumOps',
    'MetricsAfter.AppAttemptFirstContainerAllocationDelayAvgTime',
    'MetricsAfter.AllocatedMB', 'MetricsAfter.AllocatedVCores',
    'MetricsAfter.AllocatedContainers',
    'MetricsAfter.AggregateContainersAllocated',
    'MetricsAfter.AggregateContainersReleased', 'MetricsAfter.AvailableMB',
    'MetricsAfter.AvailableVCores', 'MetricsAfter.PendingMB',
    'MetricsAfter.PendingVCores', 'MetricsAfter.PendingContainers',
    # Performance metric before and after
    'PerformanceBefore', 'PerformanceAfter'
]


def hamming_similarity(s1, s2):
    """
    Hamming string similarity, based on Hamming distance
    https://en.wikipedia.org/wiki/Hamming_distance
    :param s1:
    :param s2:
    :return:
    """
    if len(s1) != len(s2):
        return .0
    return sum([ch1 == ch2 for ch1, ch2 in zip(s1, s2)]) / len(s1)


def infer_predictor_name(req):
    """
    This function attempts to decide which is the most suitable predictor
    to fulfil the user's request.
    :param req:
    :return:
    """
    # Try to guess the job predictor from the script file path
    job_script_name = os.path.basename(req.JobFilePath)

    for predictor in predictor_matchers.keys():
        file_names = predictor_matchers[predictor]['file_names']
        for f in file_names:
            if hamming_similarity(job_script_name, f) > .9:
                return predictor

    # Look at job arguments
    # TODO: it is not useful for CSV jobs only as they all have the same args

    # Look a job script content (if possible)
    # TODO: extend this mechanism to data source different from GCS
    # Instantiates a client
    storage_client = storage.Client()

    # The name for the new bucket
    fname = req.JobFilePath.replace('gs://', '')
    fname = fname.split('/')
    bucket_name = fname[0]
    blob_name = '/'.join(fname[1:])

    # Download remote blob
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob_content = blob.download_as_string(storage_client)

    # Check hamming similarity between submitted code and known scripts
    for predictor in predictor_matchers.keys():
        file_names = predictor_matchers[predictor]['source_code']
        for fname in file_names:
            if os.path.exists(fname):
                with open(fname, 'r') as f:
                    if hamming_similarity(blob_content, f.read()) > .85:
                        return predictor

    return None


def random_string(prefix='obi', suffix='-test', n=15):
    """
    Generates a random string of length n
    :param suffix:
    :param n:
    :param prefix:
    :return:
    """
    characters = string.ascii_uppercase + string.digits
    return '{}-{}'.format(prefix,
                          datetime.datetime.today().strftime('%Y-%m-%d')) \
           + ''.join(random.choice(characters) for _ in range(n)) \
           + suffix
