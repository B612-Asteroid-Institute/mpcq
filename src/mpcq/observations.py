import quivr as qv
from adam_core.time import Timestamp


class MPCObservations(qv.Table):
    requested_provid = qv.LargeStringColumn()
    primary_designation = qv.LargeStringColumn(nullable=True)
    obsid = qv.LargeStringColumn(nullable=True)
    trksub = qv.LargeStringColumn(nullable=True)
    provid = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn(nullable=True)
    obssubid = qv.LargeStringColumn(nullable=True)
    obstime = Timestamp.as_column(nullable=True)
    ra = qv.Float64Column(nullable=True)
    dec = qv.Float64Column(nullable=True)
    rmsra = qv.Float64Column(nullable=True)
    rmsdec = qv.Float64Column(nullable=True)
    mag = qv.Float64Column(nullable=True)
    rmsmag = qv.Float64Column(nullable=True)
    band = qv.LargeStringColumn(nullable=True)
    stn = qv.LargeStringColumn(nullable=True)
    updated_at = Timestamp.as_column(nullable=True)
    created_at = Timestamp.as_column(nullable=True)
    status = qv.LargeStringColumn(nullable=True)
