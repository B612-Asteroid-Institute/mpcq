import quivr as qv
from adam_core.time import Timestamp


class MPCObservations(qv.Table):
    obsid = qv.LargeStringColumn()
    primary_designation = qv.LargeStringColumn()
    trksub = qv.LargeStringColumn(nullable=True)
    provid = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn()
    obssubid = qv.LargeStringColumn(nullable=True)
    obstime = Timestamp.as_column()
    ra = qv.Float64Column()
    dec = qv.Float64Column()
    rmsra = qv.Float64Column(nullable=True)
    rmsdec = qv.Float64Column(nullable=True)
    mag = qv.Float64Column(nullable=True)
    rmsmag = qv.Float64Column(nullable=True)
    band = qv.LargeStringColumn(nullable=True)
    stn = qv.LargeStringColumn()
    updated_at = Timestamp.as_column(nullable=True)
    created_at = Timestamp.as_column(nullable=True)
    status = qv.LargeStringColumn()
