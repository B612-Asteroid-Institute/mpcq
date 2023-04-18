from dataclasses import asdict
from typing import List

import pandas as pd

from .observation import Observation


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
