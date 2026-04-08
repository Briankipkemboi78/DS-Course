"""
Data ingestion module for the Maji Ndogo farm survey dataset.

This module provides utility functions for loading and connecting to agricultural
survey data from a SQLite database and remote weather data sources. It is intended
to be used as part of a data pipeline for the Maji Ndogo farm analysis project.

Usage:
    Import this module and call the provided functions to retrieve structured
    DataFrames for further processing or modelling.

    Example:
        from data_ingestion import create_db_engine, query_data, read_from_web_CSV

Modules:
    - sqlalchemy: Used to create database engine connections.
    - logging: Provides structured logging throughout the ingestion process.
    - pandas: Used to load and return data as DataFrames.

Typical data sources:
    - SQLite database: Maji_Ndogo_farm_survey_small.db
    - Remote CSVs: Weather station data and field mapping from GitHub.
"""

from sqlalchemy import create_engine, text
import logging
import pandas as pd

# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

### START FUNCTION

def create_db_engine(db_path):
    """
    Create and return a SQLAlchemy database engine.

    Attempts to create a database engine for the given database path and
    validates the connection. Logs an error and raises an exception if the
    engine cannot be created.

    Parameters
    ----------
    db_path : str
        The connection string for the target database
        (e.g. 'sqlite:///my_database.db').

    Returns
    -------
    sqlalchemy.engine.Engine
        A SQLAlchemy engine instance connected to the specified database.

    Raises
    ------
    ImportError
        If the required database dialect or driver is not installed.
    Exception
        If the engine cannot be created for any other reason.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine  # Return the engine object if it all works well
    except ImportError:  # If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:  # If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e


def query_data(engine, sql_query):
    """
    Execute a SQL query and return the results as a pandas DataFrame.

    Runs the provided SQL query against the database connected to the given
    engine. Raises a ValueError if the query returns an empty result, and
    propagates any SQL or connection errors encountered.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        An active SQLAlchemy engine connected to the target database.
    sql_query : str
        A valid SQL query string to execute against the database.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the rows returned by the SQL query.

    Raises
    ------
    ValueError
        If the query executes successfully but returns no data.
    Exception
        If a database or connection error occurs during query execution.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e:
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e


def read_from_web_CSV(URL):
    """
    Load a CSV file from a remote URL and return it as a pandas DataFrame.

    Attempts to fetch and parse a CSV file hosted at the given URL. Logs a
    clear error message if the URL is invalid or the file cannot be retrieved,
    and raises the underlying exception for the caller to handle.

    Parameters
    ----------
    URL : str
        The full URL pointing to a publicly accessible CSV file.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the data loaded from the remote CSV file.

    Raises
    ------
    pd.errors.EmptyDataError
        If the URL does not point to a valid or non-empty CSV file.
    Exception
        If the URL is unreachable or any other error occurs during retrieval.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e

### END FUNCTION