from dataclasses import dataclass, field

from astropy.time import Time


def timestamp_from_submission_id(submission_id: str) -> Time:
    timestamp_isot = submission_id.split("_")[0]
    return Time(timestamp_isot, format="isot")


@dataclass
class Submission:
    # MPC-assigned submission ID (same as submission_id in Observation)
    id: str

    # Added-value (number of observations in submission)
    num_observations: int

    # Added-value (extracted from submission ID)
    timestamp: Time = field(init=False)

    def __post_init__(self) -> None:
        self.timestamp = timestamp_from_submission_id(self.id)
