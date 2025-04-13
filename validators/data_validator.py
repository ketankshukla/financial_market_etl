#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Validator module for validating financial market data.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class DataValidator:
    """Validates financial market data for quality and consistency."""
    
    def __init__(self, config):
        """
        Initialize the Data Validator.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.min_price = config.min_stock_price
        self.max_price = config.max_stock_price
        self.max_missing_pct = config.max_missing_percentage
    
    def validate(self, data):
        """
        Validate the processed financial data.
        
        Args:
            data (pandas.DataFrame): Processed data to validate
        
        Returns:
            pandas.DataFrame: Validated data (with invalid values handled)
        """
        if data is None or len(data) == 0:
            logger.warning("No data to validate")
            return pd.DataFrame()
        
        logger.info("Validating financial data")
        
        try:
            # Make a copy to avoid modifying the original data
            df = data.copy()
            
            # Check if this is stock price data
            if 'Symbol' in df.columns and 'Close' in df.columns:
                # Check for missing values
                missing_pct = df.isnull().mean() * 100
                logger.info(f"Missing value percentages: {missing_pct.to_dict()}")
                
                # Identify columns with too many missing values
                problem_cols = missing_pct[missing_pct > self.max_missing_pct * 100].index.tolist()
                if problem_cols:
                    logger.warning(f"Columns with excessive missing values: {problem_cols}")
                
                # Fill remaining missing values
                # For critical financial data, forward-fill is often better than mean/median
                df = df.ffill()  # forward fill
                df = df.bfill()  # then backward fill
                
                # Validate price ranges
                price_cols = ['Open', 'High', 'Low', 'Close', 'Adj_Close'] 
                price_cols = [col for col in price_cols if col in df.columns]
                
                for col in price_cols:
                    # Flag out-of-range values
                    invalid_prices = df[(df[col] < self.min_price) | (df[col] > self.max_price)]
                    if len(invalid_prices) > 0:
                        logger.warning(f"Found {len(invalid_prices)} invalid {col} prices")
                        
                        # Replace invalid values with valid values from nearby rows
                        df.loc[(df[col] < self.min_price) | (df[col] > self.max_price), col] = np.nan
                        df[col].fillna(method='ffill', inplace=True)
                        df[col].fillna(method='bfill', inplace=True)
                
                # Ensure High >= Open >= Low and High >= Close >= Low
                inconsistent_rows = df[(df['High'] < df['Low']) | 
                                       (df['High'] < df['Open']) | 
                                       (df['High'] < df['Close']) | 
                                       (df['Low'] > df['Open']) | 
                                       (df['Low'] > df['Close'])]
                
                if len(inconsistent_rows) > 0:
                    logger.warning(f"Found {len(inconsistent_rows)} rows with inconsistent price relationships")
                    
                    # Fix inconsistent price relationships
                    for idx, row in inconsistent_rows.iterrows():
                        # Find the max and min values
                        max_price = max(row['Open'], row['High'], row['Low'], row['Close'])
                        min_price = min(row['Open'], row['High'], row['Low'], row['Close'])
                        
                        # Set High to the max value
                        df.at[idx, 'High'] = max_price
                        
                        # Set Low to the min value
                        df.at[idx, 'Low'] = min_price
                
                # Validate volume data
                if 'Volume' in df.columns:
                    # Volume should be non-negative
                    invalid_volume = df[df['Volume'] < 0]
                    if len(invalid_volume) > 0:
                        logger.warning(f"Found {len(invalid_volume)} negative volume values")
                        df.loc[df['Volume'] < 0, 'Volume'] = 0
                
                # Validate calculated metrics
                if 'Daily_Return' in df.columns:
                    # Returns should be within reasonable limits (-50% to +50% daily is extreme)
                    extreme_returns = df[(df['Daily_Return'] < -0.5) | (df['Daily_Return'] > 0.5)]
                    if len(extreme_returns) > 0:
                        logger.warning(f"Found {len(extreme_returns)} extreme daily returns")
                        
                        # Flag these as potential data issues
                        # In a real system, these might need manual review
                        df['Extreme_Return_Flag'] = False
                        df.loc[(df['Daily_Return'] < -0.5) | (df['Daily_Return'] > 0.5), 'Extreme_Return_Flag'] = True
            
            # If we have economic indicator data
            elif any(col in df.columns for col in ['GDP_Growth', 'Unemployment_Rate', 'Inflation_Rate']):
                # Check for missing values
                missing_pct = df.isnull().mean() * 100
                logger.info(f"Missing value percentages: {missing_pct.to_dict()}")
                
                # Fill missing values
                df.fillna(method='ffill', inplace=True)
                df.fillna(method='bfill', inplace=True)
                
                # Validate indicator values based on reasonable ranges
                if 'GDP_Growth' in df.columns:
                    # GDP growth rarely exceeds -10% to +15%
                    invalid_gdp = df[(df['GDP_Growth'] < -10) | (df['GDP_Growth'] > 15)]
                    if len(invalid_gdp) > 0:
                        logger.warning(f"Found {len(invalid_gdp)} invalid GDP growth values")
                        df.loc[(df['GDP_Growth'] < -10) | (df['GDP_Growth'] > 15), 'GDP_Growth'] = np.nan
                        df['GDP_Growth'].fillna(method='ffill', inplace=True)
                
                if 'Unemployment_Rate' in df.columns:
                    # Unemployment rate is generally between 0% and 30%
                    invalid_unemp = df[(df['Unemployment_Rate'] < 0) | (df['Unemployment_Rate'] > 30)]
                    if len(invalid_unemp) > 0:
                        logger.warning(f"Found {len(invalid_unemp)} invalid unemployment rate values")
                        df.loc[(df['Unemployment_Rate'] < 0) | (df['Unemployment_Rate'] > 30), 'Unemployment_Rate'] = np.nan
                        df['Unemployment_Rate'].fillna(method='ffill', inplace=True)
                
                if 'Inflation_Rate' in df.columns:
                    # Inflation rate is generally between -5% and 25% in most economies
                    invalid_inf = df[(df['Inflation_Rate'] < -5) | (df['Inflation_Rate'] > 25)]
                    if len(invalid_inf) > 0:
                        logger.warning(f"Found {len(invalid_inf)} invalid inflation rate values")
                        df.loc[(df['Inflation_Rate'] < -5) | (df['Inflation_Rate'] > 25), 'Inflation_Rate'] = np.nan
                        df['Inflation_Rate'].fillna(method='ffill', inplace=True)
            
            logger.info(f"Data validation completed for {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error validating data: {str(e)}")
            raise
