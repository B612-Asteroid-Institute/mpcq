import warnings

import pyarrow as pa
import quivr as qv


class WAMOResults(qv.Table):

    requested_value = qv.LargeStringColumn()
    submission_id = qv.LargeStringColumn(nullable=True)
    submission_block_id = qv.LargeStringColumn(nullable=True)
    obsid = qv.LargeStringColumn(nullable=True)
    obssubid = qv.LargeStringColumn(nullable=True)
    status = qv.LargeStringColumn(nullable=True)
    ref = qv.LargeStringColumn(nullable=True)
    iau_desig = qv.LargeStringColumn(nullable=True)
    input_type = qv.LargeStringColumn(nullable=True)
    obs80 = qv.LargeStringColumn(nullable=True)
    status_decoded = qv.LargeStringColumn(nullable=True)
    error = qv.LargeStringColumn(nullable=True)

    @classmethod
    def from_json(cls, json_data: dict):
        """
        Convert a JSON response from the WAMO API to a WAMOResult table.

        Parameters
        ----------
        json_data : dict
            A JSON response from the WAMO API with the resquested value added as a column and
            any potential errors added as well.

        Returns
        -------
        WAMOResults
            A table of WAMO results.
        """
        wamo_results = cls.empty()

        found = json_data["found"]
        for result in found:

            for requested_value, result_list in result.items():

                submission_id = []
                submission_block_id = []
                obsid = []
                status = []
                ref = []
                iau_desig = []
                input_type = []
                obs80 = []
                obssubid = []
                status_decoded = []

                for result_i in result_list:

                    if result_i == "Note":
                        warnings.warn(f"Note found in WAMO response: {result_i}")
                        continue

                    submission_id.append(result_i["submission_id"])
                    submission_block_id.append(result_i["submission_block_id"])
                    obsid.append(result_i["obsid"])
                    status.append(result_i["status"])
                    ref.append(result_i["ref"])
                    iau_desig.append(result_i["iau_desig"])
                    input_type.append(result_i["input_type"])
                    obs80.append(result_i["obs80"])
                    obssubid.append(result_i["obssubid"])
                    status_decoded.append(result_i["status_decoded"])

                wamo_results = qv.concatenate(
                    [
                        wamo_results,
                        cls.from_kwargs(
                            requested_value=pa.repeat(
                                requested_value, len(result_list)
                            ),
                            submission_id=submission_id,
                            submission_block_id=submission_block_id,
                            obsid=obsid,
                            obssubid=obssubid,
                            status=status,
                            ref=ref,
                            iau_desig=iau_desig,
                            input_type=input_type,
                            obs80=obs80,
                            status_decoded=status_decoded,
                        ),
                    ]
                )

        not_found = json_data["not_found"]
        for not_found_i in not_found:
            wamo_results = qv.concatenate(
                [
                    wamo_results,
                    cls.from_kwargs(
                        requested_value=pa.repeat(not_found_i, 1),
                    ),
                ]
            )

        malformed = json_data["malformed"]
        for malformed_i in malformed:
            wamo_results = qv.concatenate(
                [
                    wamo_results,
                    cls.from_kwargs(
                        requested_value=[malformed_i[0]],
                        error=[malformed_i[1]],
                    ),
                ]
            )

        return wamo_results
