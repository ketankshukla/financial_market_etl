#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main entry point for the Financial Market Data ETL pipeline.
"""

import argparse
import logging
import sys
import os
from datetime import datetime

# Import local modules
from config import Config
from orchestrator import Orchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'logs', f'etl_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Financial Market Data ETL Pipeline')
    parser.add_argument('--source', choices=['csv', 'json', 'api', 'all'], default='all',
                      help='Specify the data source to process: csv, json, api, or all')
    parser.add_argument('--symbols', type=str,
                      help='Comma-separated list of stock symbols to process (e.g., AAPL,MSFT,GOOGL)')
    parser.add_argument('--start-date', type=str,
                      help='Start date for historical data in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str,
                      help='End date for historical data in YYYY-MM-DD format')
    parser.add_argument('--config', type=str, default='config.py',
                      help='Path to configuration file')
    
    return parser.parse_args()

def main():
    """Main function to run the ETL pipeline."""
    logger.info("Starting Financial Market Data ETL Pipeline")
    
    # Parse command line arguments
    args = parse_args()
    
    # Load configuration
    config = Config()
    logger.info(f"Using source: {args.source}")
    
    # Set up data source options
    data_sources = []
    if args.source == 'all':
        data_sources = ['csv', 'json', 'api']
    else:
        data_sources = [args.source]
    
    # Initialize orchestrator
    orchestrator = Orchestrator(config)
    
    # Run ETL pipeline for each data source
    for source in data_sources:
        try:
            logger.info(f"Processing {source} data source")
            orchestrator.run_etl(source, stock_symbols=args.symbols, 
                                start_date=args.start_date, end_date=args.end_date)
        except Exception as e:
            logger.error(f"Error processing {source} data source: {str(e)}")
    
    logger.info("Financial Market Data ETL Pipeline completed")

if __name__ == "__main__":
    main()
