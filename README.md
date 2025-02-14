# mpcq
#### A Python package by the Asteroid Institute, a program of the B612 Foundation  

[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue)](https://img.shields.io/badge/Python-3.11%2B-blue)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)  
[![pip - Build, Lint, Test, and Coverage](https://github.com/B612-Asteroid-Institute/mpcq/actions/workflows/pip-build-lint-test-coverage.yml/badge.svg)](https://github.com/B612-Asteroid-Institute/mpcq/actions/workflows/pip-build-lint-test-coverage.yml)
[![Documentation Status](https://readthedocs.org/projects/mpcq/badge/?version=latest)](https://mpcq.readthedocs.io/en/latest/?badge=latest)

`mpcq` is a powerful Python client library for querying and analyzing Minor Planet Center (MPC) data through Google BigQuery. This package provides efficient access to a BigQuery instance of the Small Bodies Node (SBN) replica of the MPC's Small Bodies Node database, maintained by the Asteroid Institute.

## Features

- **BigQuery Integration**: Direct access to a complete replica of the MPC database through Google BigQuery
- **Efficient Queries**: Optimized query patterns for common asteroid data access patterns
- **Rich Data Access**: Query observations, orbits, submission history, and more
- **Cross-Matching**: Tools for matching observations and finding duplicates
- **ADES Support**: Integration with ADES format for modern asteroid data exchange

## BigQuery Dataset Access

The Asteroid Institute maintains a BigQuery replica of the Minor Planet Center's Small Bodies Node database. The dataset is available through Google Cloud's Analytics Hub and requires subscription to two listings:

1. [Main MPC Dataset](https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_1950549970f)
2. [Clustered Views Dataset](https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_views_195054bbe98)

To access the dataset, you'll need:

1. A Google Cloud Platform account
2. BigQuery API access enabled
3. Subscription to both Analytics Hub listings
4. Google Cloud credentials configured

Queries will be billed according to your Google Cloud Platform account's BigQuery pricing.

## Installation

```bash
pip install mpcq
```

## Quick Start

```python
from mpcq.client import BigQueryMPCClient

# Initialize client with your subscribed dataset IDs
client = BigQueryMPCClient(
    dataset_id="your_subscribed_main_dataset_id",
    views_dataset_id="your_subscribed_views_dataset_id"
)

# Query observations for a specific object
observations = client.query_observations(["2013 RR165"])

# Convert to pandas DataFrame for analysis
from mpcq.utils import observations_to_dataframe
df = observations_to_dataframe(observations)
print(df.head())
```

## Advanced Usage

### Query Submission History
```python
# Get submission history for an object
submissions = client.query_submission_history(["2013 RR165"])
```

### Cross-Match Observations
```python
# Cross-match ADES observations with MPC database
matched = client.cross_match_observations(
    ades_observations,
    obstime_tolerance_seconds=30,
    arcseconds_tolerance=2.0
)
```

### Find Duplicates
```python
# Find potential duplicate observations
duplicates = client.find_duplicates(
    "2013 RR165",
    obstime_tolerance_seconds=30,
    arcseconds_tolerance=2.0
)
```

## Documentation

For complete documentation, including detailed API reference and examples, visit our [ReadTheDocs page](https://mpcq.readthedocs.io/).

## BigQuery Dataset Schema

The dataset (`moeyens-thor-dev.mpc_sbn_aurora`) contains several key tables:

- `public_obs_sbn`: Primary observations table
- `public_current_identifications`: Current object identifications
- `public_numbered_identifications`: Numbered asteroid identifications
- `public_orbits`: Orbital elements

You can explore these tables and their schemas directly in the [BigQuery Console](https://console.cloud.google.com/bigquery?project=moeyens-thor-dev&page=dataset&d=mpc_sbn_aurora&p=moeyens-thor-dev).

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

The Asteroid Institute acknowledges the Minor Planet Center and the Small Bodies Node for their invaluable work in maintaining the authoritative small bodies database.