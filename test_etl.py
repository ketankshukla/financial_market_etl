#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests for Financial Market ETL pipeline.
"""

import unittest
import pandas as pd
import os
import logging
from datetime import datetime, timedelta

# Import local modules
from config import Config
from extractors.csv_extractor import CSVExtractor
from extractors.json_extractor import JSONExtractor
from extractors.api_extractor import APIExtractor
from transformers.market_data_transformer import MarketDataTransformer
from transformers.metrics_calculator import MetricsCalculator
from validators.data_validator import DataValidator
from loaders.db_loader import DBLoader
from loaders.csv_loader import CSVLoader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestFinancialMarketETL(unittest.TestCase):
    """Test cases for Financial Market ETL pipeline."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = Config()
        
        # Use test data paths to avoid affecting production data
        self.config.stock_prices_csv = os.path.join(self.config.data_dir, 'stock_prices.csv')
        self.config.economic_indicators_json = os.path.join(self.config.data_dir, 'economic_indicators.json')
        
        # Initialize components
        self.csv_extractor = CSVExtractor(self.config)
        self.json_extractor = JSONExtractor(self.config)
        self.api_extractor = APIExtractor(self.config)
        self.transformer = MarketDataTransformer(self.config)
        self.metrics_calculator = MetricsCalculator(self.config)
        self.validator = DataValidator(self.config)
        self.db_loader = DBLoader(self.config)
        self.csv_loader = CSVLoader(self.config)
    
    def test_csv_extraction(self):
        """Test CSV data extraction."""
        # Extract data
        data = self.csv_extractor.extract()
        
        # Assert data is not empty
        self.assertIsNotNone(data)
        self.assertGreater(len(data), 0)
        
        # Assert expected columns are present
        expected_columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
        for col in expected_columns:
            self.assertIn(col, data.columns)
    
    def test_json_extraction(self):
        """Test JSON data extraction."""
        # Extract data
        data = self.json_extractor.extract()
        
        # Assert data is not empty
        self.assertIsNotNone(data)
        self.assertGreater(len(data), 0)
        
        # Assert expected columns are present
        expected_columns = ['date', 'indicator', 'value', 'unit', 'frequency']
        for col in expected_columns:
            self.assertIn(col, data.columns)
    
    def test_api_extraction(self):
        """Test API data extraction."""
        # Extract data with limited symbols for testing
        test_symbols = ['AAPL', 'MSFT']
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        data = self.api_extractor.extract(test_symbols, start_date, end_date)
        
        # Assert data is not empty
        self.assertIsNotNone(data)
        self.assertGreater(len(data), 0)
        
        # Assert expected columns are present
        expected_columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'Source']
        for col in expected_columns:
            self.assertIn(col, data.columns)
        
        # Assert only requested symbols are present
        self.assertTrue(all(data['Symbol'].isin(test_symbols)))
    
    def test_transformation(self):
        """Test data transformation."""
        # Extract and transform CSV data
        data = self.csv_extractor.extract()
        transformed_data = self.transformer.transform_csv_data(data)
        
        # Assert data is not empty
        self.assertIsNotNone(transformed_data)
        self.assertGreater(len(transformed_data), 0)
        
        # Assert source column was added
        self.assertIn('Source', transformed_data.columns)
        self.assertEqual(transformed_data['Source'].iloc[0], 'CSV')
    
    def test_metrics_calculation(self):
        """Test metrics calculation."""
        # Extract, transform, and calculate metrics
        data = self.csv_extractor.extract()
        transformed_data = self.transformer.transform_csv_data(data)
        metrics_data = self.metrics_calculator.calculate(transformed_data)
        
        # Assert data is not empty
        self.assertIsNotNone(metrics_data)
        self.assertGreater(len(metrics_data), 0)
        
        # Assert metrics columns were added
        metric_columns = ['Daily_Return', f'MA_{self.config.ma_short_window}', 
                         f'MA_{self.config.ma_long_window}', 'MA_Signal', 'Volatility']
        
        for col in metric_columns:
            self.assertIn(col, metrics_data.columns)
    
    def test_validation(self):
        """Test data validation."""
        # Extract, transform, calculate metrics, and validate
        data = self.csv_extractor.extract()
        transformed_data = self.transformer.transform_csv_data(data)
        metrics_data = self.metrics_calculator.calculate(transformed_data)
        validated_data = self.validator.validate(metrics_data)
        
        # Assert data is not empty
        self.assertIsNotNone(validated_data)
        self.assertGreater(len(validated_data), 0)
        
        # Assert no missing values in key columns
        key_columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close']
        for col in key_columns:
            self.assertEqual(validated_data[col].isnull().sum(), 0)

if __name__ == '__main__':
    unittest.main()
