from abc import ABC, abstractmethod


class Profile(ABC):
    """
    General abstract class which is implemented by any of the profiles made
    available to the predictive module
    """

    @property
    @abstractmethod
    def input_data(self, kwargs: dict) -> (int, int):
        """
        This method is just a getter which must be implemented by any profile
        object. It is used to return input files size and input files count for
        predictive purposes.

        The definition of this function is not bounded to a specific set of
        arguments but each profile should pass its own to better be able to
        parse the desired information.

        The desired return value is a tuple of the form (INPUT_FILES_COUNT,
        INPUT_FILES_SIZE). Even though it is not strictly enforced, different
        behaviours are strongly discouraged.

        IMPORTANT: the input_data property should not be overwritten

        :param kwargs:
        :return: tuple of two integers in the form (INPUT_FILES_COUNT,
        INPUT_FILES_SIZE)
        """

    @property
    def name(self):
        """
        Return the name identifying the current profile.

        IMPORTANT. This property MUST NOT be overwritten
        :return:
        """
        return '_'.join([
            self.job_type, self.task_type, self.backend
        ])

    @property
    @abstractmethod
    def job_type(self) -> str:
        """
        Abstract method which returns the job type for the current profile.

        IMPORTANT. This property MUST NOT be overwritten
        :return:
        """

    @property
    @abstractmethod
    def task_type(self) -> str:
        """
        Abstract method which returns the task type (if any) executed by the
        jobs corresponding to this profile.

        IMPORTANT. This property MUST NOT be overwritten
        :return:
        """


    @property
    @abstractmethod
    def backend(self) -> str:
        """
        Abstract method which returns the backend (if any) on which the
        corresponding job operates in order for a faster seeking of the
        job related information.

        IMPORTANT. This property MUST NOT be overwritten
        :return:
        """

    @abstractmethod
    def predict_duration(self, kwargs) -> float:
        """
        This is the core method of the Profile class. Indeed it is responsible
        for returning the prediction of the associated job duration.

        :param kwargs:
        :return: predicted duration for the associated job in seconds
        """
