import pandas as pd
import sqlite3
import logging
import time
import schedule
from datetime import datetime
import os
import csv
import json
from typing import Dict, List, Any, Optional, Union
import numpy as np
import io

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_etl.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("nba_etl")

class NBAStatsETL:
    """
    ETL Pipeline for NBA player statistics from CSV data.

    This class handles the extraction of data from a CSV file,
    transformation of the data using pandas, and loading into a SQLite database.
    """

    def __init__(self, db_path: str = "nba_stats.db", csv_path: str = "nba_player_stats.csv"):
        """
        Initialize the ETL pipeline.

        Args:
            db_path: Path to the SQLite database file
            csv_path: Path to the CSV file containing NBA player stats
        """
        self.db_path = db_path
        self.csv_path = csv_path

        # Create the database and tables if they don't exist
        self._initialize_database()

        logger.info("NBA Stats ETL pipeline initialized")

    def _initialize_database(self) -> None:
        """Create the database and tables if they don't exist."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Create players table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT,
                age INTEGER,
                team TEXT,
                position TEXT,
                player_additional TEXT,
                last_updated TEXT
            )
            ''')

            # Create player_stats table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER,
                season TEXT,
                games_played INTEGER,
                games_started INTEGER,
                minutes_per_game REAL,
                field_goals REAL,
                field_goal_attempts REAL,
                field_goal_pct REAL,
                three_pt_made REAL,
                three_pt_attempts REAL,
                three_pt_pct REAL,
                two_pt_made REAL,
                two_pt_attempts REAL,
                two_pt_pct REAL,
                effective_fg_pct REAL,
                ft_made REAL,
                ft_attempts REAL,
                ft_pct REAL,
                offensive_rebounds REAL,
                defensive_rebounds REAL,
                total_rebounds REAL,
                assists REAL,
                steals REAL,
                blocks REAL,
                turnovers REAL,
                personal_fouls REAL,
                points REAL,
                awards TEXT,
                last_updated TEXT,
                FOREIGN KEY (player_id) REFERENCES players (id)
            )
            ''')

            # Create etl_runs table to track pipeline executions
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS etl_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TEXT,
                end_time TEXT,
                status TEXT,
                records_processed INTEGER,
                error_message TEXT
            )
            ''')

            conn.commit()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    def extract(self) -> pd.DataFrame:
        """
        Extract player data from the CSV file.

        Returns:
            DataFrame containing player data
        """
        try:
            logger.info(f"Extracting data from CSV file: {self.csv_path}")

            # Read the CSV file
            try:
                # First, check if we're using an input CSV file
                if os.path.exists(self.csv_path):
                    df = pd.read_csv(self.csv_path)
                else:
                    # Otherwise, we need to create the file from the uploaded content
                    logger.info("CSV file not found, loading from uploaded data")
                    csv_content = self._load_uploaded_csv_content()
                    df = pd.read_csv(io.StringIO(csv_content))
            except Exception as csv_error:
                logger.error(f"Error reading CSV file: {str(csv_error)}")
                raise

            logger.info(f"Successfully extracted {len(df)} player records")
            return df
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise

    def _load_uploaded_csv_content(self) -> str:
        """
        Load the CSV content from the uploaded file.

        Returns:
            String containing CSV content
        """
        try:
            # Read from paste.txt (the uploaded file)
            with open('paste.txt', 'r', encoding='utf-8') as file:
                content = file.read()

            # Save it as a proper CSV file for future use
            with open(self.csv_path, 'w', encoding='utf-8') as file:
                file.write(content)

            logger.info(f"Successfully saved uploaded content to {self.csv_path}")
            return content
        except Exception as e:
            logger.error(f"Error loading uploaded CSV content: {str(e)}")
            raise

    def transform(self, df: pd.DataFrame) -> tuple:
        """
        Transform the raw player data from CSV.

        Args:
            df: DataFrame containing player data

        Returns:
            Tuple of (players_df, stats_df) with transformed data
        """
        try:
            logger.info("Transforming player data")

            # Clean column names
            df.columns = [col.strip() for col in df.columns]

            # Handle missing values 
            df = df.replace('-', np.nan)

            # Create a copy of the dataframe for modification
            df_clean = df.copy()

            # Separate players and stats data
            # Players table
            players_df = pd.DataFrame({
                'player_name': df_clean['Player'],
                'age': df_clean['Age'],
                'team': df_clean['Team'],
                'position': df_clean['Pos'],
                'player_additional': df_clean['Player-additional']
            })

            # Add last_updated timestamp
            players_df['last_updated'] = datetime.now().isoformat()

            # Create unique IDs for players (some players appear multiple times for different teams)
            players_df = players_df.reset_index().rename(columns={'index': 'id'})

            # Stats table
            stats_df = pd.DataFrame({
                'player_id': players_df['id'],
                'season': '2024-25',
                'games_played': df_clean['G'],
                'games_started': df_clean['GS'],
                'minutes_per_game': df_clean['MP'],
                'field_goals': df_clean['FG'],
                'field_goal_attempts': df_clean['FGA'],
                'field_goal_pct': df_clean['FG%'],
                'three_pt_made': df_clean['3P'],
                'three_pt_attempts': df_clean['3PA'],
                'three_pt_pct': df_clean['3P%'],
                'two_pt_made': df_clean['2P'],
                'two_pt_attempts': df_clean['2PA'],
                'two_pt_pct': df_clean['2P%'],
                'effective_fg_pct': df_clean['eFG%'],
                'ft_made': df_clean['FT'],
                'ft_attempts': df_clean['FTA'],
                'ft_pct': df_clean['FT%'],
                'offensive_rebounds': df_clean['ORB'],
                'defensive_rebounds': df_clean['DRB'],
                'total_rebounds': df_clean['TRB'],
                'assists': df_clean['AST'],
                'steals': df_clean['STL'],
                'blocks': df_clean['BLK'],
                'turnovers': df_clean['TOV'],
                'personal_fouls': df_clean['PF'],
                'points': df_clean['PTS'],
                'awards': df_clean['Awards']
            })

            # Add last_updated timestamp
            stats_df['last_updated'] = datetime.now().isoformat()

            logger.info(f"Data transformation complete: {len(players_df)} players, {len(stats_df)} stat records")
            return players_df, stats_df

        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            raise

    def load_players(self, players_df: pd.DataFrame) -> int:
        """
        Load transformed player data into the database.

        Args:
            players_df: DataFrame containing transformed player data

        Returns:
            Number of records inserted/updated
        """
        try:
            logger.info(f"Loading {len(players_df)} player records into database")

            conn = sqlite3.connect(self.db_path)

            # Insert records into players table
            players_df.to_sql('players', conn, if_exists='replace', index=False)

            conn.commit()
            conn.close()

            logger.info(f"Successfully loaded {len(players_df)} player records")
            return len(players_df)
        except Exception as e:
            logger.error(f"Error loading player data: {str(e)}")
            raise

    def load_stats(self, stats_df: pd.DataFrame) -> int:
        """
        Load transformed player stats into the database.

        Args:
            stats_df: DataFrame containing transformed stats data

        Returns:
            Number of records inserted
        """
        if stats_df is None or len(stats_df) == 0:
            logger.info("No stats data to load")
            return 0

        try:
            logger.info(f"Loading {len(stats_df)} stats records into database")

            conn = sqlite3.connect(self.db_path)

            # For this dataset, we're doing a full replace since it's a snapshot
            stats_df.to_sql('player_stats', conn, if_exists='replace', index=False)

            conn.commit()
            conn.close()

            logger.info(f"Successfully loaded {len(stats_df)} stats records")
            return len(stats_df)
        except Exception as e:
            logger.error(f"Error loading stats data: {str(e)}")
            raise

    def log_etl_run(self, start_time: datetime, status: str, records_processed: int, 
                   error_message: Optional[str] = None) -> None:
        """
        Log an ETL run in the database.

        Args:
            start_time: Start time of the ETL run
            status: Status of the run ('success' or 'failed')
            records_processed: Number of records processed
            error_message: Error message if any
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            end_time = datetime.now()

            cursor.execute(
                '''INSERT INTO etl_runs 
                   (start_time, end_time, status, records_processed, error_message)
                   VALUES (?, ?, ?, ?, ?)''',
                (start_time.isoformat(), end_time.isoformat(), 
                 status, records_processed, error_message)
            )

            conn.commit()
            conn.close()

            logger.info(f"ETL run logged: {status}, {records_processed} records processed")
        except Exception as e:
            logger.error(f"Error logging ETL run: {str(e)}")

    def run_pipeline(self) -> None:
        """
        Run the full ETL pipeline.
        """
        start_time = datetime.now()
        total_records = 0

        try:
            logger.info(f"Starting NBA Stats ETL pipeline run at {start_time}")

            # Extract data from CSV
            df = self.extract()

            # Transform player and stats data
            players_df, stats_df = self.transform(df)

            # Load players data
            self.load_players(players_df)
            total_records += len(players_df)

            # Load stats data
            self.load_stats(stats_df)
            total_records += len(stats_df)

            # Log successful ETL run
            self.log_etl_run(start_time, 'success', total_records)
            logger.info(f"NBA Stats ETL pipeline completed successfully. "
                       f"Processed {total_records} total records.")
        except Exception as e:
            logger.error(f"NBA Stats ETL pipeline failed: {str(e)}")
            # Log failed ETL run
            self.log_etl_run(start_time, 'failed', total_records, str(e))
            raise


def run_etl_job():
    """Function to run the ETL job, used for scheduling."""
    try:
        etl = NBAStatsETL()
        etl.run_pipeline()
        logger.info("Scheduled ETL job completed successfully")
    except Exception as e:
        logger.error(f"Scheduled ETL job failed: {str(e)}")
        logger.exception("Detailed error traceback:")

def get_player_stats_summary(db_path: str = "nba_stats.db") -> None:
    """
    Print a summary of player stats from the database.

    Args:
        db_path: Path to the SQLite database
    """
    try:
        conn = sqlite3.connect(db_path)

        # Get top 10 scorers
        query = '''
        SELECT p.player_name, ps.points, ps.total_rebounds, ps.assists, p.team
        FROM player_stats ps
        JOIN players p ON ps.player_id = p.id
        ORDER BY ps.points DESC
        LIMIT 10
        '''

        df = pd.read_sql_query(query, conn)
        print("\nTop 10 NBA Scorers:")
        print(df)

        # Get top 5 rebounders
        query = '''
        SELECT p.player_name, ps.total_rebounds, ps.points, ps.assists, p.team
        FROM player_stats ps
        JOIN players p ON ps.player_id = p.id
        ORDER BY ps.total_rebounds DESC
        LIMIT 5
        '''

        df = pd.read_sql_query(query, conn)
        print("\nTop 5 NBA Rebounders:")
        print(df)

        # Get top 5 assist leaders
        query = '''
        SELECT p.player_name, ps.assists, ps.points, ps.total_rebounds, p.team
        FROM player_stats ps
        JOIN players p ON ps.player_id = p.id
        ORDER BY ps.assists DESC
        LIMIT 5
        '''

        df = pd.read_sql_query(query, conn)
        print("\nTop 5 NBA Assist Leaders:")
        print(df)

        conn.close()
    except Exception as e:
        print(f"Error generating summary: {str(e)}")

if __name__ == "__main__":
    # Run the ETL pipeline once immediately
    logger.info("Running initial ETL job")
    run_etl_job()

    # Print a summary of the extracted data
    get_player_stats_summary()

    # Schedule the ETL job to run daily at 2 AM
    schedule.every().day.at("02:00").do(run_etl_job)

    logger.info("ETL job scheduled to run daily at 02:00")
    logger.info("In a production environment, the script would continue running to execute scheduled jobs.")
    logger.info("Script execution complete.")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute