import logging
from typing import Iterator
import sqlalchemy as sq

import numpy as np

from adam_core.coordinates.cometary import CometaryCoordinates
from adam_core.coordinates.covariances import (
    CoordinateCovariances,
    sigmas_to_covariances,
)
from adam_core.coordinates.origin import Origin
from adam_core.time import Timestamp
from adam_core.orbits import Orbits
from adam_core.orbits.query.sbdb import _convert_SBDB_covariances

logger = logging.getLogger(__name__)


def orbits_from_query_result(
    results: sq.engine.cursor.LegacyCursorResult
) -> Orbits:
    """
    Iterates through a query result and coerces the results in an
    adam_core Orbits object

    Covariance is computed from the orbital element sigmas
    """

    # covariances_mpc = np.zeros((chunk_size, 6, 6), dtype=np.float64)
    covariances_list = []
    columns = results.keys()
    result_dict = {col: [] for col in columns}

    for i, result in enumerate(results):
        # We occasionally have null orbits in the database, so we want to skip those
        if result['epoch_mjd'] is None:
            continue
        for col in columns:
            # if null we want to skip this iteration
            if col == "tp":
                result_dict[col].append(
                    Timestamp.from_mjd([result[col]], scale="tdb").mjd()[0].as_py()
                )
            else:
                result_dict[col].append(result[col])

        sigmas = np.ma.array(
            [
                [
                    result["e_sig"],
                    result["q_sig"],
                    result["tp_sig"],
                    result["raan_sig"],
                    result["ap_sig"],
                    result["i_sig"],
                ]
            ]
        )
        covariances_list.append(sigmas_to_covariances(sigmas)[0])

    covariances_mpc = np.zeros((len(covariances_list), 6, 6), dtype=np.float64)
    for i in range(len(covariances_mpc)):
        covariances_mpc[i, :, :] = sigmas_to_covariances(sigmas)[0]

    covariances_cometary = _convert_SBDB_covariances(covariances_mpc)
    times = Timestamp.from_mjd(result_dict["epoch_mjd"], scale="tdb")
    origin = Origin.from_kwargs(code=["SUN" for i in range(len(times))])
    frame = "ecliptic"
    coordinates = CometaryCoordinates.from_kwargs(
        time=times,
        q=result_dict["q"],
        e=result_dict["e"],
        i=result_dict["i"],
        raan=result_dict["raan"],
        ap=result_dict["ap"],
        tp=result_dict["tp"],
        covariance=CoordinateCovariances.from_matrix(covariances_cometary),
        origin=origin,
        frame=frame,
    )

    orbit_ids = np.array([str(id) for id in result_dict["mpc_id"]])
    object_ids = np.array(result_dict["provid"])

    return Orbits.from_kwargs(
        orbit_id=orbit_ids, object_id=object_ids, coordinates=coordinates.to_cartesian()
    )
