#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Market Data Transformer module for processing and transforming financial market data.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class MarketDataTransformer:
    """Transforms financial market data from various sources."""
    
    def __init__(self, config):
        """
        Initialize the Market Data Transformer.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
    
    def transform_csv_data(self, data):
        """
        Transform data extracted from CSV files.
        
        Args:
            data (pandas.DataFrame): Raw data from CSV
        
        Returns:
            pandas.DataFrame: Transformed data
        """
        if data is None or len(data) == 0:
            logger.warning("No CSV data to transform")
            return pd.DataFrame()
        
        logger.info("Transforming CSV data")
        
        try:
            # Make a copy to avoid modifying the original data
            df = data.copy()
            
            # Ensure date column is datetime
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            
            # Ensure numeric columns are float
            numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Fill missing values with forward fill
            df = df.ffill()
            
            # Add source column
            df['Source'] = 'CSV'
            
            # Sort by date and symbol
            df.sort_values(['Symbol', 'Date'], inplace=True)
            
            logger.info(f"Transformed {len(df)} rows of CSV data")
            return df
            
        except Exception as e:
            logger.error(f"Error transforming CSV data: {str(e)}")
            raise
    
    def transform_json_data(self, data):
        """
        Transform data extracted from JSON files.
        
        Args:
            data (pandas.DataFrame): Raw data from JSON
        
        Returns:
            pandas.DataFrame: Transformed data
        """
        if data is None or len(data) == 0:
            logger.warning("No JSON data to transform")
            return pd.DataFrame()
        
        logger.info("Transforming JSON data")
        
        try:
            # Make a copy to avoid modifying the original data
            df = data.copy()
            
            # Ensure date column is datetime
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.rename(columns={'date': 'Date'}, inplace=True)
            
            # Pivot the data to have indicators as columns
            if 'indicator' in df.columns and 'value' in df.columns:
                # Create pivot table with indicators as columns
                pivot_df = df.pivot_table(
                    index='Date', 
                    columns='indicator', 
                    values='value',
                    aggfunc='first'
                ).reset_index()
                
                # Rename columns to be more readable
                pivot_df.columns.name = None
            else:
                pivot_df = df
            
            # Add source column
            pivot_df['Source'] = 'JSON'
            
            # Sort by date
            pivot_df.sort_values('Date', inplace=True)
            
            logger.info(f"Transformed {len(pivot_df)} rows of JSON data")
            return pivot_df
            
        except Exception as e:
            logger.error(f"Error transforming JSON data: {str(e)}")
            raise
    
    def transform_api_data(self, data):
        """
        Transform data extracted from APIs.
        
        Args:
            data (pandas.DataFrame): Raw data from API
        
        Returns:
            pandas.DataFrame: Transformed data
        """
        if data is None or len(data) == 0:
            logger.warning("No API data to transform")
            return pd.DataFrame()
        
        logger.info("Transforming API data")
        
        try:
            # Make a copy to avoid modifying the original data
            df = data.copy()
            
            # Ensure date column is datetime
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            
            # Ensure numeric columns are float
            numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Calculate adjusted close if not present (in real data often available)
            if 'Close' in df.columns and 'Adj_Close' not in df.columns:
                df['Adj_Close'] = df['Close']
            
            # Add source column if not present
            if 'Source' not in df.columns:
                df['Source'] = 'API'
            
            # Sort by date and symbol
            df.sort_values(['Symbol', 'Date'], inplace=True)
            
            logger.info(f"Transformed {len(df)} rows of API data")
            return df
            
        except Exception as e:
            logger.error(f"Error transforming API data: {str(e)}")
            raise
    
    def merge_dataframes(self, dfs):
        """
        Merge multiple dataframes into one.
        
        Args:
            dfs (list): List of dataframes to merge
        
        Returns:
            pandas.DataFrame: Merged dataframe
        """
        # Filter out empty dataframes
        valid_dfs = [df for df in dfs if df is not None and len(df) > 0]
        
        if not valid_dfs:
            logger.warning("No valid dataframes to merge")
            return pd.DataFrame()
        
        logger.info(f"Merging {len(valid_dfs)} dataframes")
        
        try:
            # Concatenate all dataframes
            merged_df = pd.concat(valid_dfs, ignore_index=True)
            
            # Remove duplicates
            merged_df.drop_duplicates(subset=['Date', 'Symbol'], keep='first', inplace=True)
            
            logger.info(f"Merged dataframe has {len(merged_df)} rows")
            return merged_df
            
        except Exception as e:
            logger.error(f"Error merging dataframes: {str(e)}")
            raise
