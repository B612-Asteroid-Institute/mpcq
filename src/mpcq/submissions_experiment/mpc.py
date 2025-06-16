from abc import ABC, abstractmethod


class MPCSubmissionClient(ABC):

    @abstractmethod
    def submit_ades():
        pass

    @abstractmethod
    def submit_identifications():
        pass
