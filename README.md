# Data Warehouses with AWS

## Overview
Utilize AWS to create a STAR schema data warehouse

## File Context
create_tables.py:  Runs drop and create tables statements
etl.py: Loads and inserts data into tables
sql_queries.py: Holds SQL statements called in create_tables.py and etl.py to create, drop, and insert tables
***configs file not shared to Github for information security purposes, but a basic .py file holding config information should also be configured for each project

## Database Schema
**Fact Table**
  songplays - records in event data associated with song plays i.e. records with page NextSong
      - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
      
**Dimension Tables**
  users - users in the app
    - user_id, first_name, last_name, gender, level
  songs - songs in music database
    - song_id, title, artist_id, year, duration
  artists - artists in music database
    - artist_id, name, location, lattitude, longitude
  time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

## Instructions to Run
1.  Open new terminal.
2.  Run "python create_tables.py"
3.  Run "python etl.py"
4.  Query your new database!

## Credits and Acknowledgements
This project was completed as part of Udacity's Data Engineering Nanodegree.
