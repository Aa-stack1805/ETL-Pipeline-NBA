# NBA Player Stats ETL Pipeline
A complete Extract-Transform-Load (ETL) pipeline for NBA player statistics that processes data from CSV files into a structured SQLite database. This project demonstrates how to build a production-ready data pipeline in Python.

Features

1. Extract: Read NBA player statistics from CSV files
2. Transform: Clean, restructure, and enhance the data
3. Load: Store in a SQLite database with proper relationships
4. Analyze: Query and visualize the processed data
5. Schedule: Automated data refreshes
6. Log: Comprehensive tracking and error handling

Table of Contents

Installation
Usage
Project Structure
How It Works
Database Schema
ETL Process
Error Handling and Logging
Scheduling
Extending the Pipeline


Installation

Clone this repository:

bashgit clone https://github.com/yourusername/nba-stats-etl.git
cd nba-stats-etl

Install required dependencies:

bashpip install pandas numpy matplotlib seaborn schedule
Usage

Place your NBA player stats CSV file in the root directory as paste.txt
Run the ETL pipeline:

bashpython nba_etl_complete.py

View the results in the nba_stats.db SQLite database:

bashsqlite3 nba_stats.db

Run SQL queries to explore the data:

sqlSELECT p.player_name, ps.points, ps.assists, p.team 
FROM player_stats ps 
JOIN players p ON ps.player_id = p.id 
ORDER BY ps.points DESC 
LIMIT 10;
Project Structure
nba-stats-etl/
├── nba_etl_complete.py      # Main ETL script
├── paste.txt                # Input CSV file with NBA player stats
├── nba_stats.db             # SQLite database (created when script runs)
├── nba_etl.log              # Log file (created when script runs)
├── README.md                # This documentation
└── requirements.txt         # Python dependencies
How It Works
This ETL pipeline follows a standard pattern with three major components:
Extract

Reads data from NBA player stats CSV file
Handles missing values and encoding issues
Validates input data format

Transform

Cleans column names and data values
Separates player data from statistics
Restructures data for relational database storage
Adds timestamps and additional metadata

Load

Creates database tables if they don't exist
Inserts processed data into SQLite database
Maintains proper table relationships

Database Schema
The pipeline creates three tables in the SQLite database:
players Table

id: Unique player identifier
player_name: Full player name
age: Player's age
team: Team abbreviation (e.g., LAL, BOS)
position: Player position (PG, SG, SF, PF, C)
player_additional: Additional player identifier
last_updated: Timestamp of last data update

player_stats Table

id: Unique stats record identifier
player_id: Foreign key to players table
season: NBA season (e.g., "2024-25")
games_played: Number of games played
games_started: Number of games started
minutes_per_game: Average minutes per game
Various statistics: Points, rebounds, assists, etc.
last_updated: Timestamp of last data update

etl_runs Table

id: Unique run identifier
start_time: When the ETL job started
end_time: When the ETL job completed
status: Success or failure status
records_processed: Number of records processed
error_message: Error information if failed

ETL Process
Extraction Phase
The extraction phase involves reading data from the CSV file. The code handles this through:

First checking if the specified CSV file exists
If not, reading from the uploaded paste.txt file
Converting the content to a pandas DataFrame

Key methods:

extract(): Main extraction method
_load_uploaded_csv_content(): Helper for loading uploaded file

Transformation Phase
The transformation phase is where data cleaning and restructuring happens:

Clean column names (removing whitespace)
Handle missing values (replace '-' with NaN)
Separate player information from statistics
Create database IDs and foreign key relationships
Add timestamp metadata

Key method:

transform(): Performs all transformation steps

Loading Phase
The loading phase inserts the transformed data into the SQLite database:

Connect to the SQLite database
Replace existing data with new data
Ensure proper relationships between tables
Commit the transaction and close connection

Key methods:

load_players(): Loads player information
load_stats(): Loads player statistics

Error Handling and Logging
The pipeline implements comprehensive error handling and logging:

Uses Python's logging module for consistent logs
Captures detailed exception information
Logs to both file and console
Records ETL run information in the database

Each stage of the pipeline is wrapped in try-except blocks to ensure errors are properly captured and don't cause the entire pipeline to fail without explanation.
Scheduling
The pipeline includes scheduling capabilities using the schedule library:

By default, scheduled to run daily at 2 AM
Can be modified for different frequencies
Records each run in the database

The scheduler is implemented in the main section of the script, and in a production environment, the script would continue running to execute scheduled jobs.
Extending the Pipeline
Here are some ways to extend this pipeline:

Multiple Data Sources: Add support for additional data sources like APIs
Incremental Loading: Implement change tracking to only update changed records
Data Validation: Add more robust validation rules
Visualization: Create dashboards with matplotlib, seaborn, or plotly
More Advanced Scheduling: Use Airflow or other orchestration tools
Cloud Integration: Modify to work with cloud databases like PostgreSQL or BigQuery
