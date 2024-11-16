import json

import pyarrow.compute as pc
import quivr as qv
from adam_core.orbit_determination import FittedOrbits
from astropy.time import Time


class Identifications(qv.Table):
    submission_id = qv.LargeStringColumn()
    orbit_id = qv.LargeStringColumn()
    trksub = qv.LargeStringColumn()
    obs_id = qv.LargeStringColumn()
    mpc_obs_id = qv.LargeStringColumn(nullable=True)
    mpc_trksub = qv.LargeStringColumn(nullable=True)
    days = qv.Int64Column()
    nanos = qv.Int64Column()
    observatory_code = qv.LargeStringColumn()

    def itf(self):
        return self.apply_mask(pc.invert(pc.is_null(self.mpc_obs_id)))


def identifications_to_json_string(
    identifications: Identifications,
    orbits: FittedOrbits,
    name: str,
    email: str,
    comment: str,
) -> str:
    """
    Write the identifications table to a JSON string that can be submitted to the MPC.

    Parameters
    ----------
    identifications : Identifications
        The identifications table.
    orbits : FittedOrbits
        The orbits table which is used to get the cometary elements needed
        to submit the identifications.
    name : str
        The name of the person submitting the identifications. (e.g. "J. Doe")
    email : str
        The email address of the person submitting the identifications.
    comment : str
        A comment to include with the submission.

    Returns
    -------
    str
        The identifications table as a JSON string.
    """
    identifications_json = {
        "header": {"name": name, "email": email, "comment": comment}
    }

    links = {}
    for i, orbit_id in enumerate(identifications.orbit_id.unique()):
        link_id = f"link_{i:05d}"
        orbit_identifications = identifications.select("orbit_id", orbit_id)
        orbit_trksubs = orbit_identifications.drop_duplicates(
            ["orbit_id", "trksub", "days", "mpc_trksub", "observatory_code"]
        )

        # Extract the trksubs for this orbit and add them
        # to the list of trksubs for this link
        trksubs = []
        for trksub in orbit_trksubs:

            mpc_trksub = trksub.mpc_trksub[0].as_py()
            if mpc_trksub is not None:
                trksub_id = mpc_trksub
            else:
                trksub_id = trksub.trksub[0].as_py()

            night = Time(trksub.days[0].as_py(), format="mjd", scale="utc").isot.split(
                "T"
            )[0]
            night = night.replace("-", "")

            trksubs.append([trksub_id, night, trksub.observatory_code[0].as_py()])

        orbit = orbits.select("orbit_id", orbit_id)
        if len(orbit) != 1:
            raise ValueError("Orbit not found in orbits table")

        # Convert orbit to cometary elements and add
        # the orbit solution for these observations
        cometary = orbit.coordinates.to_cometary()

        links[link_id] = {
            "trksubs": trksubs,
            "orbit": {
                "arg_pericenter": cometary.ap[0].as_py(),
                "eccentricity": cometary.e[0].as_py(),
                "epoch": cometary.time.rescale("tt").jd()[0].as_py(),
                "inclination": cometary.i[0].as_py(),
                "lon_asc_node": cometary.raan[0].as_py(),
                "pericenter_distance": cometary.q[0].as_py(),
                "pericenter_time": cometary.tp[0].as_py() + 2400000.5,
            },
        }

    identifications_json["links"] = links

    return json.dumps(identifications_json, indent=2)
