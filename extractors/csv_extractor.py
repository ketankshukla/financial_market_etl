#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CSV Extractor module for extracting financial market data from CSV files.
"""

import logging
import pandas as pd
import os

logger = logging.getLogger(__name__)

class CSVExtractor:
    """Extracts financial market data from CSV files."""
    
    def __init__(self, config):
        """
        Initialize the CSV Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.source_file = config.stock_prices_csv
    
    def extract(self):
        """
        Extract data from CSV file.
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            FileNotFoundError: If the CSV file doesn't exist
        """
        logger.info(f"Extracting data from CSV file: {self.source_file}")
        
        # Check if file exists
        if not os.path.exists(self.source_file):
            # If the file doesn't exist, create a sample file for demo purposes
            logger.warning(f"CSV file not found: {self.source_file}. Creating sample data.")
            self._create_sample_data()
        
        try:
            # Read CSV file
            df = pd.read_csv(self.source_file, parse_dates=['Date'])
            
            # Basic info about the extracted data
            logger.info(f"Extracted {len(df)} rows from CSV file")
            logger.info(f"Columns in CSV data: {', '.join(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from CSV file: {str(e)}")
            raise
    
    def _create_sample_data(self):
        """Create sample stock price data for demonstration purposes."""
        import numpy as np
        from datetime import datetime, timedelta
        
        # Generate dates for the past year
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        dates = pd.date_range(start=start_date, end=end_date, freq='B')  # Business days
        
        # Sample stock symbols
        symbols = self.config.default_symbols
        
        # Create empty list to store data
        data = []
        
        # Generate random stock price data
        for symbol in symbols:
            # Start with a base price between 50 and 500
            base_price = np.random.uniform(50, 500)
            
            # Generate daily price movements with some randomness
            price = base_price
            for date in dates:
                # Daily price change (-2% to +2%)
                daily_change = np.random.normal(0.0005, 0.015)
                price = price * (1 + daily_change)
                
                # Add some volume
                volume = int(np.random.normal(1000000, 500000))
                if volume < 100000:
                    volume = 100000
                
                # Add to data list
                data.append({
                    'Date': date.strftime('%Y-%m-%d'),
                    'Symbol': symbol,
                    'Open': round(price * (1 - np.random.uniform(0, 0.005)), 2),
                    'High': round(price * (1 + np.random.uniform(0, 0.01)), 2),
                    'Low': round(price * (1 - np.random.uniform(0, 0.01)), 2),
                    'Close': round(price, 2),
                    'Volume': volume
                })
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
        
        # Save to CSV
        df.to_csv(self.source_file, index=False)
        logger.info(f"Created sample stock price data: {self.source_file}")
