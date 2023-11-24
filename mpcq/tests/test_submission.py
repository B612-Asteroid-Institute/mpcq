from astropy.time import Time

from mpcq.submission import timestamp_from_submission_id


def test_timestamp_from_submission_id():
    submission_id = "2020-10-23T06:39:01.200_0000DvZH"
    desired_timestamp = Time("2020-10-23T06:39:01.200", format="isot")

    assert timestamp_from_submission_id(submission_id) == desired_timestamp
