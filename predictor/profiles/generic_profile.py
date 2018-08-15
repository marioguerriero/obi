from abc import ABC, abstractmethod


class GenericProfile(ABC):
    """
    General abstract class which is implemented by any of the prediction profiles
    made available for OBI
    """

    @abstractmethod
    def generate_predictions(self, metrics):
        """
        This function generate and returns a tuple containing job duration and
        failure probability predictions. The user should only pass to this
        function a snapshot of the metrics for the cluster on which he is
        trying to execute the job. The class instance should be able to
        autonomously obtain any other information which may be needed to
        produce its output.
        :param metrics:
        :return tuple(int, float):
        """
