# Project Introduction
A music streaming startup, __Sparkify__, has grown its user base and song database even more and wants to move its data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights about what songs their users are listening to.

I'll be able to test my database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare my results with their expected results.

# Data Flow Diagram on AWS
![image](https://user-images.githubusercontent.com/80867381/236677391-93aa7f50-fd6e-460a-ac95-2451a3621df9.png)

### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

#### Example of song data files
        `song_data/A/B/C/TRABCEI128F424C983.json`
        `song_data/A/A/B/TRAABJL12903CDCF1A.json`

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

`{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`


### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.
The log files in the dataset you'll be working with are partitioned by year and month.

#### Example of log data files
        `log_data/2018/11/2018-11-12-events.json
         log_data/2018/11/2018-11-13-events.json`
         
 ![log-data](https://user-images.githubusercontent.com/80867381/235442887-f6298846-7541-4043-982f-a4d53a4640c8.png)

# Song Plays Analytical Tables
### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page `NextSong`
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
2. users - users in the app
  - user_id, first_name, last_name, gender, level
3. songs - songs in music database
  - song_id, title, artist_id, year, duration
4. artists - artists in music database
  - artist_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
  - start_time, hour, day, week, month, year, weekday

![sparkifydb_erd](https://user-images.githubusercontent.com/80867381/214580187-78bda55c-c1ed-4296-8614-0dab5892df16.png)

# Steps
1. Build and Configure project infrastructure and resources on aws using terraform

2. Load the data from s3 Buckets

3. Develop complete ETL pipeline for logs and songs data using spark 

4. test the result aginst defined queries

# Project Files
- `Infrastructure-terraform` Build and Configure project infrastructure and resources on aws using terraform
- `sparkify-etl.py` reads data from S3, processes that data using Spark, and writes them back to S3
- `dl.cfg` contains your AWS credentials
- `README.md` provides discussion on data processing and decisions
- `udf.py` define helper udf functions to use it with spark in the ETL process
- `sparkify-notebook.ipynb` Jupyter notebook to explore data and build and test the etl pipeline 
