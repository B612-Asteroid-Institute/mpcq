import decimal

import pandas as pd
from astropy.time import Time

from mpcq.observation import Observation, ObservationStatus
from mpcq.submission import Submission
from mpcq.utils import observations_to_dataframe, submissions_to_dataframe


def test_observations_to_dataframe():

    observation = Observation(
        mpc_id=1000,
        status=ObservationStatus.Published,
        obscode="I41",
        filter_band="R",
        unpacked_provisional_designation="2022 AJ2",
        timestamp=Time("2021-01-01T00:00:00.000", format="isot"),
        ra=decimal.Decimal(1.0),
        ra_rms=decimal.Decimal(0.001),
        dec=decimal.Decimal(-10.0),
        dec_rms=decimal.Decimal(0.004),
        mag=decimal.Decimal(20.0),
        mag_rms=decimal.Decimal(0.1),
        submission_id="2020-10-23T06:39:01.200_0000DvZH",
        created_at=Time("2020-10-23T06:39:01.200", format="isot"),
        updated_at=Time("2020-10-23T06:39:01.200", format="isot"),
    )

    desired_df = pd.DataFrame(
        {
            "mpc_id": [1000],
            "status": ["Published"],
            "obscode": ["I41"],
            "filter_band": ["R"],
            "unpacked_provisional_designation": ["2022 AJ2"],
            "timestamp": [Time("2021-01-01T00:00:00.000", format="isot")],
            "ra": [decimal.Decimal(1.0)],
            "ra_rms": [decimal.Decimal(0.001)],
            "dec": [decimal.Decimal(-10.0)],
            "dec_rms": [decimal.Decimal(0.004)],
            "mag": [decimal.Decimal(20.0)],
            "mag_rms": [decimal.Decimal(0.1)],
            "submission_id": ["2020-10-23T06:39:01.200_0000DvZH"],
            "created_at": [Time("2020-10-23T06:39:01.200", format="isot")],
            "updated_at": [Time("2020-10-23T06:39:01.200", format="isot")],
        }
    )

    pd.testing.assert_frame_equal(observations_to_dataframe([observation]), desired_df)


def test_submissions_to_dataframe():

    submission = Submission(
        id="2020-10-23T06:39:01.200_0000DvZH",
        num_observations=5,
    )

    desired_df = pd.DataFrame(
        {
            "id": ["2020-10-23T06:39:01.200_0000DvZH"],
            "num_observations": [5],
            "timestamp": [Time("2020-10-23T06:39:01.200", format="isot")],
        }
    )

    pd.testing.assert_frame_equal(submissions_to_dataframe([submission]), desired_df)
