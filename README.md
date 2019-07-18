
## Using the Files
- Modify `dwh.cfg` with AWS key and secret key.
- Running `1_create_cluster.py` from the terminal will create a cluster based on the parameters in the config file.  It will also modify the the file with the IAM arn and the new endpoint for the cluster.
- `2_create_tables.py` is set up for testing.  Running this 'as-is' from the terminal will drop all the tables and then re-create them.
- Running `3_etl.py` will pull all the data from the S3 buckets in the config file and load them into staging tables in Redshift and then transform the staging tables into the star schema shown above.
- Running `4_terminate_cluster.py` will terminate the cluster.  Note that arguments are currently set to NOT create a snapshot.