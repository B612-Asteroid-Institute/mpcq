import decimal

import pandas as pd

from mpcq.observation import Observation, ObservationStatus
from mpcq.utils import observations_to_dataframe


def test_observations_to_dataframe():

    observation = Observation(
        mpc_id=1000,
        status=ObservationStatus.Published,
        obscode="I41",
        filter_band="R",
        unpacked_provisional_designation="2022 AJ2",
        timestamp="2021-01-01T00:00:00.000",
        ra=decimal.Decimal(1.0),
        ra_rms=decimal.Decimal(0.001),
        dec=decimal.Decimal(-10.0),
        dec_rms=decimal.Decimal(0.004),
        mag=decimal.Decimal(20.0),
        mag_rms=decimal.Decimal(0.1),
    )

    desired_df = pd.DataFrame(
        {
            "mpc_id": [1000],
            "status": ["Published"],
            "obscode": ["I41"],
            "filter_band": ["R"],
            "unpacked_provisional_designation": ["2022 AJ2"],
            "timestamp": ["2021-01-01T00:00:00.000"],
            "ra": [decimal.Decimal(1.0)],
            "ra_rms": [decimal.Decimal(0.001)],
            "dec": [decimal.Decimal(-10.0)],
            "dec_rms": [decimal.Decimal(0.004)],
            "mag": [decimal.Decimal(20.0)],
            "mag_rms": [decimal.Decimal(0.1)],
        }
    )

    pd.testing.assert_frame_equal(observations_to_dataframe([observation]), desired_df)
