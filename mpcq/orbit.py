import logging

import numpy as np
import sqlalchemy as sq
from adam_core.coordinates.cartesian import CartesianCoordinates
from adam_core.coordinates.covariances import (
    CoordinateCovariances,
)
from adam_core.coordinates.origin import Origin
from adam_core.orbits import Orbits
from adam_core.time import Timestamp

logger = logging.getLogger(__name__)


def orbits_from_query_result(results: sq.engine.cursor.LegacyCursorResult) -> Orbits:
    """
    Iterates through a query result with jsonb and coerces the results to an
    adam_core Orbits object

    Covariance is computed by MPC and is included
    """

    # covariances_mpc = np.zeros((chunk_size, 6, 6), dtype=np.float64)
    covariances_list = []
    result_dict = {
        "mpc_id": [],
        "provid": [],
        "epoch_mjd": [],
    }
    cart_coeff = []

    for i, result in enumerate(results):
        # We occasionally have null orbits in the database, so we want to skip those
        if result["mpc_orb_jsonb"] is None:
            continue
        cart_coeff.append(result["mpc_orb_jsonb"]["CAR"]["coefficient_values"])
        cart_covariance = np.zeros((6, 6), dtype=np.float64)
        # MPC supplies a 9x9 upper triangular matrix, this indexes over the jsonb
        # and sets the values to the format expected by adam_core
        for j in range(6):
            for k in range(6 - j):
                cart_covariance[j, k] = cart_covariance[k, j] = result["mpc_orb_jsonb"][
                    "CAR"
                ]["covariance"][f"cov{j}{k+j}"]

        covariances_list.append(cart_covariance)

        result_dict["mpc_id"].append(result["mpc_id"])
        result_dict["provid"].append(result["provid"])
        result_dict["epoch_mjd"].append(result["mpc_orb_jsonb"]["epoch_data"]["epoch"])

    covariances_cartesian = np.zeros((len(covariances_list), 6, 6), dtype=np.float64)
    for i in range(len(covariances_list)):
        covariances_cartesian[i, :, :] = covariances_list[i]

    times = Timestamp.from_mjd(result_dict["epoch_mjd"], scale="tt")
    origin = Origin.from_kwargs(code=["SUN" for i in range(len(times))])
    frame = "ecliptic"
    cart_coeff = np.array(cart_coeff)
    coordinates = CartesianCoordinates.from_kwargs(
        time=times,
        x=cart_coeff[:, 0],
        y=cart_coeff[:, 1],
        z=cart_coeff[:, 2],
        vx=cart_coeff[:, 3],
        vy=cart_coeff[:, 4],
        vz=cart_coeff[:, 5],
        covariance=CoordinateCovariances.from_matrix(covariances_cartesian),
        origin=origin,
        frame=frame,
    )

    orbit_ids = np.array([str(id) for id in result_dict["mpc_id"]])
    object_ids = np.array(result_dict["provid"])

    return Orbits.from_kwargs(
        orbit_id=orbit_ids, object_id=object_ids, coordinates=coordinates
    )
