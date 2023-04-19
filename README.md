# mpcq
#### A Python package by the Asteroid Institute, a program of the B612 Foundation  

This is a client library for interacting with an MPC observations database.

## Usage

To connect to the Asteroid Institute's clone of the Small Bodies Node MPC database:
```python
from mpcq.client import MPCObservationsClient

client = MPCObservationsClient.connect_using_gcloud()
```

With a client initialized, you can get all observations of a particular object using its provisional designation:
```python
observations = client.get_object_observations("2013 RR165")
observations = list(observations)
```

These can be converted into a dataframe as follows:

```python
from mpcq.utils import observations_to_dataframe

observations_df = observations_to_dataframe(observations)
print(observations_df.head(5))
```
```
	mpc_id	status	obscode	filter_band	unpacked_provisional_designation	timestamp	ra	ra_rms	dec	dec_rms	mag	mag_rms	submission_id	created_at	updated_at
0	174511900	Published	F51	w	2013 RR165	2011-01-30 11:15:26	123.884679	None	19.820047	None	22.2	None	2011-04-12T00:57:19.000_00005L9j	2017-07-10 00:00:00.000000	2022-06-15 17:17:33.421485
1	174511901	Published	F51	w	2013 RR165	2011-01-30 11:37:23	123.880767	None	19.820603	None	22.4	None	2011-04-12T00:57:19.000_00005L9j	2017-07-10 00:00:00.000000	2022-06-15 17:17:33.427512
2	174511902	Published	F51	w	2013 RR165	2011-01-30 12:22:35	123.872683	None	19.821694	None	22.3	None	2011-04-12T00:57:19.000_00005L9j	2017-07-10 00:00:00.000000	2022-06-15 17:17:33.431369
3	394474985	Published	W84	g	2013 RR165	2013-09-02 05:49:09	354.49745627	0.097	1.17373181	0.100	21.66	0.07	2022-05-23T23:16:35.633_0000EfpX	2022-05-23 23:18:28.963374	2022-06-15 17:17:33.434170
4	175542203	Published	F51	w	2013 RR165	2013-09-03 10:20:19	354.259229	None	1.099064	None	21.6	None	2013-09-04T00:37:51.000_00005cQy	2017-07-10 00:00:00.000000	2022-06-15 17:17:33.436820
```

Getting the submission ID and the number of observations per submission of an object:
```python
submissions = client.get_object_submissions("2013 RR165")
submissions = list(submissions)
```

As before, these can be converted to a dataframe:
```python
from mpcq.utils import submissions_to_dataframe

submissions_df = submissions_to_dataframe(submissions)
print(submissions_df)
```
```
	id	num_observations	timestamp
0	2017-07-05T20:57:57.001_00006dS8	3	2017-07-05 20:57:57.001
1	2013-09-04T00:37:51.000_00005cQy	3	2013-09-04 00:37:51.000
2	2017-09-13T19:53:09.000_0000CdSX	2	2017-09-13 19:53:09.000
3	2022-05-23T23:16:35.633_0000EfpX	19	2022-05-23 23:16:35.633
4	2013-09-12T11:43:25.001_00005cpl	4	2013-09-12 11:43:25.001
5	2011-04-12T00:57:19.000_00005L9j	3	2011-04-12 00:57:19.000
6	2016-04-03T19:54:29.000_00006IpM	3	2016-04-03 19:54:29.000
7	2015-01-16T23:14:02.000_00005wg5	3	2015-01-16 23:14:02.000
```
