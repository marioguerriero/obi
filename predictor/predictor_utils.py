import datetime
import random
import string

predictor_matchers = {
    'csv_find': {
        'file_names': ['find_changed']
    },
    'csv_update': {
            'file_names': ['update']
    },
    'csv_recreate': {
        'file_names': ['recreate_file']
    },
    'ulm': {
        'file_names': ['ulm']
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
    'MetricsBefore.AggregateContainersPreempted', 'MetricsBefore.ActiveApplications',
    'MetricsBefore.AppAttemptFirstContainerAllocationDelayNumOps',
    'MetricsBefore.AppAttemptFirstContainerAllocationDelayAvgTime',
    'MetricsBefore.AllocatedMB', 'MetricsBefore.AllocatedVCores',
    'MetricsBefore.AllocatedContainers', 'MetricsBefore.AggregateContainersAllocated',
    'MetricsBefore.AggregateContainersReleased', 'MetricsBefore.AvailableMB',
    'MetricsBefore.AvailableVCores', 'MetricsBefore.PendingMB',
    'MetricsBefore.PendingVCores', 'MetricsBefore.PendingContainers',
    # Metrics after scaling
    'MetricsAfter.AMResourceLimitMB', 'MetricsAfter.AMResourceLimitVCores',
    'MetricsAfter.UsedAMResourceMB', 'MetricsAfter.UsedAMResourceVCores',
    'MetricsAfter.AppsSubmitted', 'MetricsAfter.AppsRunning',
    'MetricsAfter.AppsPending', 'MetricsAfter.AppsCompleted',
    'MetricsAfter.AppsKilled', 'MetricsAfter.AppsFailed',
    'MetricsAfter.AggregateContainersPreempted', 'MetricsAfter.ActiveApplications',
    'MetricsAfter.AppAttemptFirstContainerAllocationDelayNumOps',
    'MetricsAfter.AppAttemptFirstContainerAllocationDelayAvgTime',
    'MetricsAfter.AllocatedMB', 'MetricsAfter.AllocatedVCores',
    'MetricsAfter.AllocatedContainers', 'MetricsAfter.AggregateContainersAllocated',
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
    job_path = req.JobFilePath.split('/')[-1]
    job_script_name = job_path.split('.')[-1]

    for predictor in predictor_matchers.keys():
        file_names = predictor_matchers[predictor]['file_names']
        for f in file_names:
            if hamming_similarity(job_script_name, f) > .9:
                return predictor

    # Look at job arguments
    # TODO

    # Look a job script content (if possible)
    # TODO

    return None


def random_string(prefix='obi', suffix='-test', n=15):
    """
    Generates a random string of length n
    :param suffix:
    :param n:
    :param prefix:
    :return:
    """
    return '{}-{}'.format(prefix,
                          datetime.datetime.today().strftime('%Y-%m-%d')) \
           + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n)) \
           + suffix
