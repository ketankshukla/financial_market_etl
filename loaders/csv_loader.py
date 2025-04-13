#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CSV Loader module for exporting financial market data to CSV files.
"""

import logging
import os
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class CSVLoader:
    """Exports financial market data to CSV files."""
    
    def __init__(self, config):
        """
        Initialize the CSV Loader.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.output_dir = config.processed_data_dir
    
    def export(self, data):
        """
        Export data to CSV files.
        
        Args:
            data (pandas.DataFrame): Data to export
        
        Returns:
            bool: True if successful, False otherwise
        """
        if data is None or len(data) == 0:
            logger.warning("No data to export to CSV")
            return False
        
        logger.info(f"Exporting {len(data)} rows to CSV")
        
        try:
            # Create output directory if it doesn't exist
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Get current timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Determine file name based on data structure
            if 'Symbol' in data.columns and 'Close' in data.columns:
                # For stock price data, create a file per symbol
                symbols = data['Symbol'].unique()
                
                for symbol in symbols:
                    symbol_data = data[data['Symbol'] == symbol].copy()
                    
                    # Create filename for this symbol
                    filename = os.path.join(self.output_dir, f"{symbol}_prices_{timestamp}.csv")
                    
                    # Export to CSV
                    symbol_data.to_csv(filename, index=False)
                    logger.info(f"Exported {len(symbol_data)} rows for {symbol} to {filename}")
                
                # Also export a consolidated file
                all_data_filename = os.path.join(self.output_dir, f"all_stock_prices_{timestamp}.csv")
                data.to_csv(all_data_filename, index=False)
                logger.info(f"Exported consolidated data to {all_data_filename}")
                
            elif any(col in data.columns for col in ['GDP_Growth', 'Unemployment_Rate', 'Inflation_Rate']):
                # For economic indicators
                filename = os.path.join(self.output_dir, f"economic_indicators_{timestamp}.csv")
                data.to_csv(filename, index=False)
                logger.info(f"Exported {len(data)} rows of economic indicators to {filename}")
                
            else:
                # Generic financial data
                filename = os.path.join(self.output_dir, f"financial_data_{timestamp}.csv")
                data.to_csv(filename, index=False)
                logger.info(f"Exported {len(data)} rows of financial data to {filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error exporting data to CSV: {str(e)}")
            return False
