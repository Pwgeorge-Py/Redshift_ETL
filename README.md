<h2>Project goals</h2>

The purpose of this pipeline and database is to gain analysis insights on the consumer habits who use the music streaming app Sparkify.
Data is collected by the app in JSON format and is stored in an AWS S3 bucket.
The 2 source datasets are: 
Songs dataset which stores information about all the songs/artists on the platform.
Log dataset which stores information about activities users are performing on the platform such as listening to a song.

The end goal here is to get this data into a format that can allow a user to perform analytical queries to hopefully derive buisness insights.


<h2>Design Decisions</h2>

<h4>Database schema</h4>
Our destination database will be in a STAR schema format, this contains 1 fact table with a number of dimension tables directly connected to it.
A STAR schema gives us a simple database design that also gives us a faster performance of queries.
From that 2 source datasets I have created 5 destination tables:
Songplays
Users
Songs
Artists
Time

These tables are created by the 'create_tables.py' python script,
Queries to create and insert data into tables are stored in the 'sql_queries.py' script

For most columns in these tables the data is represented in the same format that occurs in the source datasets, however there are a few exceptions:
start_time - This is now a Timestamp, derived from the EPOCH time 'ts' in the Log dataset
level      - This is now a boolean, derived from 'level' in the Log dataset as it can only have 2 values (Paid = 1 or Free = 0)
gender     - This is now a boolean, derived from 'gender' in the Log dataset as it can only have 2 values (M = 1 or F = 0)
Time       - (Table) The values in this dimension table are all derived from the original Timstamp value


<h4>ETL pipeline</h4>

The ETL pipeline is done through a python script named "etl.py"

This script systematically executes SQL queries that first extracts the raw data and puts it into a staging database,
This allows us to keep a source record of extracted data we can go back to if anything goes wrong when transforming the data for our star schema.

After data has been successfully put into staging tables, the script will execute a series of SQL queries to extract the required data for insert into the star schema tables that will be used for querying.

