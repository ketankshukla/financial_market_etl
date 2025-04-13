#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configuration settings for the Financial Market Data ETL pipeline.
"""

import os
from datetime import datetime, timedelta

class Config:
    """Configuration class for ETL pipeline settings."""
    
    def __init__(self):
        # Base paths
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(self.base_dir, 'data')
        self.logs_dir = os.path.join(self.base_dir, 'logs')
        
        # Ensure directories exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        
        # Input data paths
        self.stock_prices_csv = os.path.join(self.data_dir, 'stock_prices.csv')
        self.economic_indicators_json = os.path.join(self.data_dir, 'economic_indicators.json')
        
        # Output data paths
        self.processed_data_dir = os.path.join(self.data_dir, 'processed')
        os.makedirs(self.processed_data_dir, exist_ok=True)
        
        self.output_csv = os.path.join(self.processed_data_dir, f'financial_data_{datetime.now().strftime("%Y%m%d")}.csv')
        
        # Database settings
        self.db_type = 'sqlite'  # 'sqlite', 'mysql', 'postgresql'
        self.db_path = os.path.join(self.data_dir, 'financial_market.db')
        self.db_connection_string = f'sqlite:///{self.db_path}'
        
        # API settings
        self.alpha_vantage_api_key = os.environ.get('ALPHA_VANTAGE_API_KEY', 'demo')
        self.alpha_vantage_base_url = 'https://www.alphavantage.co/query'
        
        # Default query parameters
        self.default_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
        self.default_start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        self.default_end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Transformation parameters
        self.ma_short_window = 20  # Short-term moving average window
        self.ma_long_window = 50   # Long-term moving average window
        self.volatility_window = 20  # Volatility calculation window
        
        # Validation thresholds
        self.min_stock_price = 0.01
        self.max_stock_price = 100000
        self.max_missing_percentage = 0.1  # Maximum allowed percentage of missing values
