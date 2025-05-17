# NBA Player Stats ETL Pipeline

A complete Extract-Transform-Load (ETL) pipeline for NBA player statistics that processes data from CSV files into a structured SQLite database. This project demonstrates how to build a production-ready data pipeline in Python.

![NBA Stats ETL](https://img.shields.io/badge/NBA-Stats%20ETL-blue)
![Python](https://img.shields.io/badge/Python-3.6%2B-brightgreen)
![License](https://img.shields.io/badge/License-MIT-yellow)

## Features

- **Extract**: Read NBA player statistics from CSV files
- **Transform**: Clean, restructure, and enhance the data
- **Load**: Store in a SQLite database with proper relationships
- **Analyze**: Query and visualize the processed data
- **Schedule**: Automated data refreshes
- **Log**: Comprehensive tracking and error handling

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)
- [Database Schema](#database-schema)
- [ETL Process](#etl-process)
- [Error Handling and Logging](#error-handling-and-logging)
- [Scheduling](#scheduling)
- [Extending the Pipeline](#extending-the-pipeline)
- [License](#license)

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/nba-stats-etl.git
cd nba-stats-etl
```

2. Install required dependencies:
```bash
pip install pandas numpy matplotlib seaborn schedule
```

## Usage

1. Place your NBA player stats CSV file in the root directory as `paste.txt`

2. Run the ETL pipeline:
```bash
python nba_etl_complete.py
```

3. View the results in the `nba_stats.db` SQLite database:
```bash
sqlite3 nba_stats.db
```

4. Run SQL queries to explore the data:
```sql
SELECT p.player_name, ps.points, ps.assists, p.team 
FROM player_stats ps 
JOIN players p ON ps.player_id = p.id 
ORDER BY ps.points DESC 
LIMIT 10;
```

## Project Structure

```
nba-stats-etl/
├── nba_etl_complete.py      # Main ETL script
├── paste.txt                # Input CSV file with NBA player stats
├── nba_stats.db             # SQLite database (created when script runs)
├── nba_etl.log              # Log file (created when script runs)
├── README.md                # This documentation
└── requirements.txt         # Python dependencies
```

## How It Works

This ETL pipeline follows a standard pattern with three major components:

### Extract
- Reads data from NBA player stats CSV file
- Handles missing values and encoding issues
- Validates input data format

### Transform
- Cleans column names and data values
- Separates player data from statistics
- Restructures data for relational database storage
- Adds timestamps and additional metadata

### Load
- Creates database tables if they don't exist
- Inserts processed data into SQLite database
- Maintains proper table relationships

## Database Schema

The pipeline creates three tables in the SQLite database:

### `players` Table
- `id`: Unique player identifier
- `player_name`: Full player name
- `age`: Player's age
- `team`: Team abbreviation (e.g., LAL, BOS)
- `position`: Player position (PG, SG, SF, PF, C)
- `player_additional`: Additional player identifier
- `last_updated`: Timestamp of last data update

### `player_stats` Table
- `id`: Unique stats record identifier
- `player_id`: Foreign key to players table
- `season`: NBA season (e.g., "2024-25")
- `games_played`: Number of games played
- `games_started`: Number of games started
- `minutes_per_game`: Average minutes per game
- *Various statistics*: Points, rebounds, assists, etc.
- `last_updated`: Timestamp of last data update

### `etl_runs` Table
- `id`: Unique run identifier
- `start_time`: When the ETL job started
- `end_time`: When the ETL job completed
- `status`: Success or failure status
- `records_processed`: Number of records processed
- `error_message`: Error information if failed

## ETL Process

### Extraction Phase

The extraction phase involves reading data from the CSV file. The code handles this through:

1. First checking if the specified CSV file exists
2. If not, reading from the uploaded `paste.txt` file
3. Converting the content to a pandas DataFrame

Key methods:
- `extract()`: Main extraction method
- `_load_uploaded_csv_content()`: Helper for loading uploaded file

### Transformation Phase

The transformation phase is where data cleaning and restructuring happens:

1. Clean column names (removing whitespace)
2. Handle missing values (replace '-' with NaN)
3. Separate player information from statistics
4. Create database IDs and foreign key relationships
5. Add timestamp metadata

Key method:
- `transform()`: Performs all transformation steps

### Loading Phase

The loading phase inserts the transformed data into the SQLite database:

1. Connect to the SQLite database
2. Replace existing data with new data
3. Ensure proper relationships between tables
4. Commit the transaction and close connection

Key methods:
- `load_players()`: Loads player information
- `load_stats()`: Loads player statistics

## Error Handling and Logging

The pipeline implements comprehensive error handling and logging:

- Uses Python's `logging` module for consistent logs
- Captures detailed exception information
- Logs to both file and console
- Records ETL run information in the database

Each stage of the pipeline is wrapped in try-except blocks to ensure errors are properly captured and don't cause the entire pipeline to fail without explanation.

## Scheduling

The pipeline includes scheduling capabilities using the `schedule` library:

- By default, scheduled to run daily at 2 AM
- Can be modified for different frequencies
- Records each run in the database

The scheduler is implemented in the main section of the script, and in a production environment, the script would continue running to execute scheduled jobs.

## Extending the Pipeline

Here are some ways to extend this pipeline:

1. **Multiple Data Sources**: Add support for additional data sources like APIs
2. **Incremental Loading**: Implement change tracking to only update changed records
3. **Data Validation**: Add more robust validation rules
4. **Visualization**: Create dashboards with matplotlib, seaborn, or plotly
5. **More Advanced Scheduling**: Use Airflow or other orchestration tools
6. **Cloud Integration**: Modify to work with cloud databases like PostgreSQL or BigQuery

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

Created by [Your Name] - Feel free to contribute!
