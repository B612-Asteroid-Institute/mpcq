import pyarrow.compute as pc
import quivr as qv


class SubmissionDetails(qv.Table):
    orbit_id = qv.LargeStringColumn()
    trksub = qv.LargeStringColumn()
    obssubid = qv.LargeStringColumn()
    submission_id = qv.LargeStringColumn()

    def trksub_mapping(
        self, mpc_submission_info: "MPCSubmissionInfo"
    ) -> "TrksubMapping":
        """
        Create a mapping of trksub to primary designation, provid, permid, submission ID for these
        submission details.

        Parameters
        ----------
        mpc_submission_info : MPCSubmissionInfo
            Table of submission results from the MPC. See `MPCClient.query_submission_info`.

        Returns
        -------
        TrksubMapping
            Table of trksub mappings. Each trksub will for each unique primary designation it
            was linked to by the MPC.
        """
        assert pc.all(pc.is_in(mpc_submission_info.trksub, self.trksub)).as_py()

        unique_submission_details = self.drop_duplicates(
            ["orbit_id", "trksub", "submission_id"]
        )

        unique_mappings = self.drop_duplicates(
            ["trksub", "primary_designation", "provid", "permid", "submission_id"]
        )

        trksub_mapping = (
            unique_submission_details.table.join(
                unique_mappings.table,
                ("trksub", "submission_id"),
                ("trksub", "submission_id"),
            )
            .select(
                [
                    "trksub",
                    "primary_designation",
                    "provid",
                    "permid",
                    "submission_id",
                    "orbit_id",
                ]
            )
            .sort_by(
                [
                    ("trksub", "ascending"),
                    ("submission_id", "ascending"),
                    ("primary_designation", "ascending"),
                ]
            )
        )
        return TrksubMapping.from_pyarrow(trksub_mapping)


class TrksubMapping(qv.Table):

    trksub = qv.LargeStringColumn()
    primary_designation = qv.LargeStringColumn(nullable=True)
    provid = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn()
    orbit_id = qv.LargeStringColumn()


class MPCSubmissionInfo(qv.Table):
    obsid = qv.LargeStringColumn()
    obssubid = qv.LargeStringColumn(nullable=True)
    primary_designation = qv.LargeStringColumn(nullable=True)
    trksub = qv.LargeStringColumn()
    provid = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn()
    status = qv.LargeStringColumn()
