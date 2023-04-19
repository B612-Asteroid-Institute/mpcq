from dataclasses import asdict
from typing import List

import pandas as pd

from .observation import Observation
from .submission import Submission


def observations_to_dataframe(observations: List[Observation]) -> pd.DataFrame:
    """
    Convert a list of Observation objects to a pandas DataFrame.

    Parameters
    ----------
    observations : List[Observation]
        The observations to convert.

    Returns
    -------
    observations : `~pd.DataFrame`
    """
    data = [asdict(obs) for obs in observations]

    # Convert ObservationStatus objects to strings.
    for row in data:
        row["status"] = row["status"].name

    return pd.DataFrame(data)


def submissions_to_dataframe(submissions: List[Submission]) -> pd.DataFrame:
    """
    Convert a list of Submission objects to a pandas DataFrame.

    Parameters
    ----------
    submissions : List[Submission]
        The submissions to convert.

    Returns
    -------
    submissions : `~pd.DataFrame`
    """
    data = [asdict(sub) for sub in submissions]
    return pd.DataFrame(data)
