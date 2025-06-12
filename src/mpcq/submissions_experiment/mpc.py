from abc import ABC, abstractmethod


class MPCSubmission(ABC):

    @abstractmethod
    def submit_ades():
        pass

    @abstractmethod
    def submit_identifications():
        pass
