# Sparkify ETL

## Project Description
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal here is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.


## Usage
In order to run this ETL

1. Create a Redshift Cluster by running the cells in the `create_redshift_cluster.ipynb` notebook.
2. Create tables on Redshift by simply running `python create_table.py` file through terminal.
3. Run `python etl.py` to populate the dimension and fact tables.
4. Finally, run the delete cluster 

## Files Description


- `create_redshift_cluster.ipynb`: A jupyter notebook that contains infrastructure as code including examples for creating s3 buckets, roles and redshift clusters locally.
- `delete_redshift_cluster.ipynb`: A jupyter notebook that contains infrastructure as code for deleting the redshift cluster. 
- `sql_queries.py`: This file contains all sql queries for creating staging tables and the star schema tables.

- `create_tables.py`: A python file to create the schemas for staging and star schema tables.
- `etl.py`: A python file for loading the data to the staging tables, applying transformation and loading the result to the dimension and fact tables.


