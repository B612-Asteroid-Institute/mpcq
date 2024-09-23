import quivr as qv



class MPCSubmissionInfo(qv.Table):
    obsid = qv.LargeStringColumn()
    obssubid = qv.LargeStringColumn(nullable=True)
    primary_designation = qv.LargeStringColumn(nullable=True)
    trksub = qv.LargeStringColumn()
    provid = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn()
    status = qv.LargeStringColumn()
