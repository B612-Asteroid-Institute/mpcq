import datetime

import numpy as np


def round_to_nearest_millisecond(t: datetime.datetime) -> datetime.datetime:
    microseconds = np.ceil(t.microsecond / 1000).astype(int) * 1000
    return t.replace(microsecond=microseconds)
