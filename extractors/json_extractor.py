#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
JSON Extractor module for extracting financial market data from JSON files.
"""

import logging
import json
import pandas as pd
import os

logger = logging.getLogger(__name__)

class JSONExtractor:
    """Extracts financial market data from JSON files."""
    
    def __init__(self, config):
        """
        Initialize the JSON Extractor.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.source_file = config.economic_indicators_json
    
    def extract(self):
        """
        Extract data from JSON file.
        
        Returns:
            pandas.DataFrame: Extracted data
        
        Raises:
            FileNotFoundError: If the JSON file doesn't exist
        """
        logger.info(f"Extracting data from JSON file: {self.source_file}")
        
        # Check if file exists
        if not os.path.exists(self.source_file):
            # If the file doesn't exist, create a sample file for demo purposes
            logger.warning(f"JSON file not found: {self.source_file}. Creating sample data.")
            self._create_sample_data()
        
        try:
            # Read JSON file
            with open(self.source_file, 'r') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            df = pd.DataFrame(data['indicators'])
            
            # Convert date strings to datetime objects
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            
            # Basic info about the extracted data
            logger.info(f"Extracted {len(df)} rows from JSON file")
            logger.info(f"Columns in JSON data: {', '.join(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting data from JSON file: {str(e)}")
            raise
    
    def _create_sample_data(self):
        """Create sample economic indicators data for demonstration purposes."""
        import numpy as np
        from datetime import datetime, timedelta
        
        # Generate dates for the past year
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # Monthly start
        
        # List of economic indicators
        indicators = [
            {"indicator": "GDP_Growth", "unit": "percent", "frequency": "quarterly"},
            {"indicator": "Unemployment_Rate", "unit": "percent", "frequency": "monthly"},
            {"indicator": "Inflation_Rate", "unit": "percent", "frequency": "monthly"},
            {"indicator": "Interest_Rate", "unit": "percent", "frequency": "monthly"},
            {"indicator": "Consumer_Confidence", "unit": "index", "frequency": "monthly"}
        ]
        
        # Generate data for each indicator
        data = []
        
        for indicator in indicators:
            indicator_name = indicator["indicator"]
            frequency = indicator["frequency"]
            
            # Base value and volatility based on indicator type
            if indicator_name == "GDP_Growth":
                base_value = 2.5  # ~2.5% annual growth
                volatility = 0.3
                # Only use quarterly dates for GDP
                indicator_dates = pd.date_range(start=start_date, end=end_date, freq='QS')
            elif indicator_name == "Unemployment_Rate":
                base_value = 4.0  # ~4% unemployment
                volatility = 0.2
                indicator_dates = dates
            elif indicator_name == "Inflation_Rate":
                base_value = 2.0  # ~2% inflation
                volatility = 0.1
                indicator_dates = dates
            elif indicator_name == "Interest_Rate":
                base_value = 1.5  # ~1.5% interest rate
                volatility = 0.05
                indicator_dates = dates
            else:  # Consumer_Confidence
                base_value = 100  # Index starting at 100
                volatility = 3
                indicator_dates = dates
            
            # Generate values with random walk
            value = base_value
            for date in indicator_dates:
                # Add some randomness to the value
                change = np.random.normal(0, volatility)
                value += change
                
                # Ensure values make sense (e.g., unemployment and inflation can't be negative)
                if indicator_name in ["Unemployment_Rate", "Inflation_Rate", "Interest_Rate"] and value < 0:
                    value = 0.1
                
                # Add to data
                data.append({
                    "date": date.strftime('%Y-%m-%d'),
                    "indicator": indicator_name,
                    "value": round(value, 2),
                    "unit": indicator["unit"],
                    "frequency": frequency
                })
        
        # Create JSON structure
        json_data = {
            "metadata": {
                "source": "Sample Economic Indicators",
                "description": "Sample economic indicators for financial market analysis",
                "last_updated": datetime.now().strftime('%Y-%m-%d')
            },
            "indicators": data
        }
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.source_file), exist_ok=True)
        
        # Save to JSON file
        with open(self.source_file, 'w') as f:
            json.dump(json_data, f, indent=2)
        
        logger.info(f"Created sample economic indicators data: {self.source_file}")
