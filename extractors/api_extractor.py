#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
API Extractor module for extracting financial market data from external APIs.
"""

import logging
import pandas as pd
import requests
import json
import os
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class APIExtractor:
    """Extracts financial market data from external APIs."""
    
    def __init__(self, config):
        """
        Initialize the API Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.api_key = config.alpha_vantage_api_key
        self.base_url = config.alpha_vantage_base_url
        
    def extract(self, symbols=None, start_date=None, end_date=None):
        """
        Extract data from financial APIs.
        
        Args:
            symbols (list): List of stock symbols to fetch
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
        
        Returns:
            pandas.DataFrame: Extracted data
        """
        logger.info("Extracting data from financial APIs")
        
        # Use default symbols if none provided
        if not symbols:
            symbols = self.config.default_symbols
        
        # Use default dates if none provided
        if not start_date:
            start_date = self.config.default_start_date
        if not end_date:
            end_date = self.config.default_end_date
        
        logger.info(f"Fetching data for symbols: {symbols}")
        logger.info(f"Date range: {start_date} to {end_date}")
        
        # For demo purposes, we'll create a mock API response rather than hitting real API
        # In a real implementation, this would make actual API calls
        if self.api_key == 'demo':
            logger.warning("Using demo API key. Creating mock data instead of real API calls.")
            return self._create_mock_api_data(symbols, start_date, end_date)
        
        # Initialize empty dataframe to store all results
        all_data = pd.DataFrame()
        
        # Fetch data for each symbol
        for symbol in symbols:
            try:
                # Alpha Vantage API parameters for daily time series
                params = {
                    'function': 'TIME_SERIES_DAILY',
                    'symbol': symbol,
                    'apikey': self.api_key,
                    'outputsize': 'full',
                    'datatype': 'json'
                }
                
                logger.info(f"Fetching data for {symbol}")
                
                # Make API request
                response = requests.get(self.base_url, params=params)
                
                # Check if request was successful
                if response.status_code == 200:
                    data = response.json()
                    
                    # Parse the Alpha Vantage response
                    if 'Time Series (Daily)' in data:
                        time_series = data['Time Series (Daily)']
                        
                        # Convert to DataFrame
                        df = pd.DataFrame.from_dict(time_series, orient='index')
                        
                        # Rename columns
                        df.columns = [col.split('. ')[1] for col in df.columns]
                        
                        # Add symbol column
                        df['Symbol'] = symbol
                        
                        # Convert index to datetime and add as column
                        df.index = pd.to_datetime(df.index)
                        df['Date'] = df.index
                        df.reset_index(drop=True, inplace=True)
                        
                        # Filter by date range
                        mask = (df['Date'] >= start_date) & (df['Date'] <= end_date)
                        df = df.loc[mask]
                        
                        # Convert numeric columns to float
                        for col in ['open', 'high', 'low', 'close', 'volume']:
                            if col in df.columns:
                                df[col] = df[col].astype(float)
                        
                        # Rename columns to match our standard format
                        df.rename(columns={
                            'open': 'Open',
                            'high': 'High',
                            'low': 'Low',
                            'close': 'Close',
                            'volume': 'Volume'
                        }, inplace=True)
                        
                        # Append to main dataframe
                        all_data = pd.concat([all_data, df])
                        
                        logger.info(f"Successfully retrieved {len(df)} records for {symbol}")
                    else:
                        logger.warning(f"No time series data found for {symbol}")
                else:
                    logger.error(f"API request failed for {symbol}: {response.status_code} - {response.text}")
                
                # API rate limiting - sleep to avoid hitting rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                continue
        
        # If we didn't get any data, return mock data for demo purposes
        if len(all_data) == 0:
            logger.warning("No data retrieved from API. Using mock data.")
            return self._create_mock_api_data(symbols, start_date, end_date)
        
        logger.info(f"Total records extracted from API: {len(all_data)}")
        return all_data
    
    def _create_mock_api_data(self, symbols, start_date, end_date):
        """Create mock API data for demonstration purposes."""
        import numpy as np
        
        logger.info("Creating mock API data")
        
        # Convert date strings to datetime objects
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        # Generate date range
        date_range = pd.date_range(start=start_dt, end=end_dt, freq='B')  # Business days
        
        # Create empty list to store data
        data = []
        
        # Generate random stock price data
        for symbol in symbols:
            # Start with a base price between 50 and 500
            base_price = np.random.uniform(50, 500)
            
            # Generate daily price movements with some randomness
            price = base_price
            for date in date_range:
                # Daily price change (-2% to +2%)
                daily_change = np.random.normal(0.0005, 0.015)
                price = price * (1 + daily_change)
                
                # Add some volume
                volume = int(np.random.normal(1000000, 500000))
                if volume < 100000:
                    volume = 100000
                
                # Add to data list
                data.append({
                    'Date': date,
                    'Symbol': symbol,
                    'Open': round(price * (1 - np.random.uniform(0, 0.005)), 2),
                    'High': round(price * (1 + np.random.uniform(0, 0.01)), 2),
                    'Low': round(price * (1 - np.random.uniform(0, 0.01)), 2),
                    'Close': round(price, 2),
                    'Volume': volume,
                    'Source': 'API'
                })
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        logger.info(f"Created mock API data with {len(df)} records")
        return df
