#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Database Loader module for loading financial market data into databases.
"""

import logging
import os
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, DateTime, MetaData
from datetime import datetime

logger = logging.getLogger(__name__)

class DBLoader:
    """Loads financial market data into databases."""
    
    def __init__(self, config):
        """
        Initialize the Database Loader.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.connection_string = config.db_connection_string
        self.db_path = config.db_path
    
    def load(self, data):
        """
        Load data into database.
        
        Args:
            data (pandas.DataFrame): Data to load
        
        Returns:
            bool: True if successful, False otherwise
        """
        if data is None or len(data) == 0:
            logger.warning("No data to load into database")
            return False
        
        logger.info(f"Loading {len(data)} rows into database")
        
        try:
            # Create database directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Create engine based on connection string
            engine = create_engine(self.connection_string)
            
            # Determine table name based on data structure
            if 'Symbol' in data.columns and 'Close' in data.columns:
                table_name = 'stock_prices'
            elif any(col in data.columns for col in ['GDP_Growth', 'Unemployment_Rate', 'Inflation_Rate']):
                table_name = 'economic_indicators'
            else:
                table_name = 'financial_data'
            
            # Add a timestamp for when this data was loaded
            data['load_timestamp'] = datetime.now()
            
            # Load data to database
            data.to_sql(table_name, engine, if_exists='append', index=False)
            
            logger.info(f"Successfully loaded data into {table_name} table")
            
            # Return query example for user reference
            self._log_query_examples(table_name)
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading data into database: {str(e)}")
            return False
    
    def _log_query_examples(self, table_name):
        """Log example queries for the loaded data."""
        if table_name == 'stock_prices':
            logger.info("Example queries for stock_prices table:")
            logger.info("SELECT * FROM stock_prices WHERE Symbol = 'AAPL' ORDER BY Date DESC LIMIT 10;")
            logger.info("SELECT Symbol, AVG(Close) as avg_price, MAX(Close) as max_price FROM stock_prices GROUP BY Symbol;")
        
        elif table_name == 'economic_indicators':
            logger.info("Example queries for economic_indicators table:")
            logger.info("SELECT * FROM economic_indicators ORDER BY Date DESC LIMIT 10;")
            logger.info("SELECT AVG(Inflation_Rate) as avg_inflation, AVG(Unemployment_Rate) as avg_unemployment FROM economic_indicators;")
        
        else:
            logger.info("Example queries for financial_data table:")
            logger.info("SELECT * FROM financial_data ORDER BY Date DESC LIMIT 10;")
    
    def query_data(self, query):
        """
        Execute a query on the database.
        
        Args:
            query (str): SQL query to execute
        
        Returns:
            pandas.DataFrame: Query results
        """
        try:
            engine = create_engine(self.connection_string)
            return pd.read_sql(query, engine)
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return None
