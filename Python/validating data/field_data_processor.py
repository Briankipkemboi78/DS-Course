"""
Field data processor module for the Maji Ndogo farm survey dataset.

This module defines the FieldDataProcessor class, which provides a complete
pipeline for ingesting, cleaning, and enriching field-level agricultural survey
data. It combines data from a SQLite database with remote weather station
mapping data to produce a single, analysis-ready DataFrame.

Usage:
    Import this module and instantiate the FieldDataProcessor class with a
    configuration dictionary, then call the .process() method to run the
    full pipeline.

    Example:
        from field_data_processor import FieldDataProcessor

        config_params = {
            "db_path": "sqlite:///Maji_Ndogo_farm_survey_small.db",
            "sql_query": "SELECT * FROM geographic_features ...",
            "columns_to_rename": {"Annual_yield": "Crop_type", "Crop_type": "Annual_yield"},
            "values_to_rename": {"cassaval": "cassava", "wheatn": "wheat", "teaa": "tea"},
            "weather_csv_path": "https://...",
            "weather_mapping_csv": "https://...",
        }

        processor = FieldDataProcessor(config_params)
        processor.process()
        df = processor.df

Dependencies:
    - pandas: Used for data manipulation and merging.
    - data_ingestion: Provides create_db_engine, query_data, and read_from_web_CSV.
    - logging: Provides structured, instance-level logging.
"""

import logging
import pandas as pd

from data_ingestion import create_db_engine, query_data, read_from_web_CSV


class FieldDataProcessor:
    """
    A data processing pipeline for Maji Ndogo field survey data.

    This class ingests raw field data from a SQLite database, applies a series
    of cleaning and correction steps, and merges in weather station mapping
    data to produce a single enriched DataFrame.

    Parameters
    ----------
    config_params : dict
        A dictionary of configuration parameters for the pipeline. Expected keys:
            - db_path (str): SQLAlchemy connection string for the SQLite database.
            - sql_query (str): SQL query used to extract data from the database.
            - columns_to_rename (dict): Mapping of column names to swap.
            - values_to_rename (dict): Mapping of incorrect crop name strings to correct ones.
            - weather_mapping_csv (str): URL to the weather station mapping CSV file.
    logging_level : str, optional
        Logging verbosity level. Accepted values are "DEBUG", "INFO", or "NONE".
        Defaults to "INFO".

    Attributes
    ----------
    df : pd.DataFrame or None
        The processed DataFrame. None until .process() or .ingest_sql_data() is called.
    engine : sqlalchemy.engine.Engine or None
        The database engine. None until .ingest_sql_data() is called.

    Examples
    --------
    >>> processor = FieldDataProcessor(config_params)
    >>> processor.process()
    >>> processor.df.head()
    """

    def __init__(self, config_params, logging_level="INFO"):
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        self.initialize_logging(logging_level)

        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """
        Set up logging for this instance of FieldDataProcessor.

        Configures a dedicated logger scoped to this class instance, with a
        console handler and a timestamped log format. Log propagation to the
        root logger is disabled to avoid duplicate messages.

        Parameters
        ----------
        logging_level : str
            Desired logging level. Accepted values are "DEBUG", "INFO", or
            "NONE". Any unrecognised value defaults to "INFO".
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents duplicate messages in the root logger

        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging entirely
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO for unrecognised values

        self.logger.setLevel(log_level)

        if not self.logger.handlers:  # Avoid adding duplicate handlers on re-instantiation
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def ingest_sql_data(self):
        """
        Load field survey data from the SQLite database into a DataFrame.

        Creates a database engine using the configured db_path, executes the
        configured SQL query, and stores the result in self.df.

        Returns
        -------
        pd.DataFrame
            The raw DataFrame loaded from the database.

        Raises
        ------
        Exception
            If the database engine cannot be created or the query fails.
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Successfully loaded data.")
        return self.df

    def rename_columns(self):
        """
        Swap two mislabelled column names in the DataFrame.

        Uses a temporary column name as an intermediate step to safely swap
        the two column names defined in self.columns_to_rename without
        creating a naming conflict.
        """
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]

        # Use a temp name to avoid overwriting one column before the other is renamed
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:  # Ensure the temp name does not clash with existing columns
            temp_name += "_"

        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
        self.logger.info(f"Swapped columns: {column1} with {column2}")

    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Apply data corrections to crop type labels and elevation values.

        Replaces misspelled crop type strings using the values_to_rename
        mapping, leaving unrecognised values unchanged. Also converts any
        negative elevation values to their absolute value.

        Parameters
        ----------
        column_name : str, optional
            Name of the column containing crop type labels. Defaults to 'Crop_type'.
        abs_column : str, optional
            Name of the column containing elevation values. Defaults to 'Elevation'.
        """
        self.df[abs_column] = self.df[abs_column].abs()  # Fix negative elevation values
        self.df[column_name] = self.df[column_name].apply(
            lambda crop: self.values_to_rename.get(crop, crop)  # Replace typos; leave valid values unchanged
        )

    def weather_station_mapping(self):
        """
        Merge weather station mapping data into the main DataFrame.

        Fetches the weather station mapping CSV from the configured URL and
        performs a left merge onto self.df using Field_ID as the join key,
        enriching the dataset with weather station identifiers.
        """
        weather_map_df = read_from_web_CSV(self.weather_map_data)
        self.df = self.df.merge(weather_map_df, on='Field_ID', how='left')
        self.logger.info("Weather station mapping merged successfully.")

    def process(self):
        """
        Run the full data processing pipeline in order.

        Sequentially calls each processing method to produce a cleaned and
        enriched DataFrame stored in self.df:

            1. ingest_sql_data      — load raw data from the database
            2. rename_columns       — swap mislabelled column names
            3. apply_corrections    — fix elevation signs and crop name typos
            4. weather_station_mapping — merge in weather station identifiers
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()