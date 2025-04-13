#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Metrics Calculator module for computing financial metrics and indicators.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class MetricsCalculator:
    """Calculates financial metrics from transformed market data."""
    
    def __init__(self, config):
        """
        Initialize the Metrics Calculator.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.short_window = config.ma_short_window
        self.long_window = config.ma_long_window
        self.volatility_window = config.volatility_window
    
    def calculate(self, csv_data=None, json_data=None, api_data=None):
        """
        Calculate financial metrics for the given data from multiple sources.
        
        Args:
            csv_data (pandas.DataFrame, optional): Transformed CSV data
            json_data (pandas.DataFrame, optional): Transformed JSON data
            api_data (pandas.DataFrame, optional): Transformed API data
        
        Returns:
            pandas.DataFrame: Combined data with additional metrics
        """
        # Combine data from different sources, ignoring None values
        dfs = []
        if csv_data is not None and not csv_data.empty:
            dfs.append(csv_data)
        if json_data is not None and not json_data.empty:
            dfs.append(json_data)
        if api_data is not None and not api_data.empty:
            dfs.append(api_data)
            
        if not dfs:
            logger.warning("No data to calculate metrics")
            return pd.DataFrame()
            
        # Combine all available dataframes
        data = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        
        logger.info("Calculating financial metrics")
        
        try:
            # Make a copy to avoid modifying the original data
            df = data.copy()
            
            # Check if this is stock price data (has Symbol, Open, Close, etc.)
            if 'Symbol' in df.columns and 'Close' in df.columns:
                # Group by symbol to calculate metrics for each stock
                symbols = df['Symbol'].unique()
                result_dfs = []
                
                for symbol in symbols:
                    symbol_df = df[df['Symbol'] == symbol].copy()
                    
                    # Sort by date
                    symbol_df.sort_values('Date', inplace=True)
                    
                    # Calculate returns
                    symbol_df['Daily_Return'] = symbol_df['Close'].pct_change()
                    
                    # Calculate moving averages
                    symbol_df[f'MA_{self.short_window}'] = symbol_df['Close'].rolling(window=self.short_window).mean()
                    symbol_df[f'MA_{self.long_window}'] = symbol_df['Close'].rolling(window=self.long_window).mean()
                    
                    # Calculate moving average crossover signal
                    symbol_df['MA_Signal'] = 0
                    symbol_df.loc[symbol_df[f'MA_{self.short_window}'] > symbol_df[f'MA_{self.long_window}'], 'MA_Signal'] = 1
                    symbol_df.loc[symbol_df[f'MA_{self.short_window}'] < symbol_df[f'MA_{self.long_window}'], 'MA_Signal'] = -1
                    
                    # Calculate volatility (standard deviation of returns)
                    symbol_df['Volatility'] = symbol_df['Daily_Return'].rolling(window=self.volatility_window).std() * np.sqrt(252)  # Annualized
                    
                    # Calculate Relative Strength Index (RSI)
                    delta = symbol_df['Close'].diff()
                    gain = delta.where(delta > 0, 0)
                    loss = -delta.where(delta < 0, 0)
                    
                    avg_gain = gain.rolling(window=14).mean()
                    avg_loss = loss.rolling(window=14).mean()
                    
                    rs = avg_gain / avg_loss
                    symbol_df['RSI'] = 100 - (100 / (1 + rs))
                    
                    # Calculate Bollinger Bands
                    symbol_df['BB_Middle'] = symbol_df['Close'].rolling(window=20).mean()
                    symbol_df['BB_Std'] = symbol_df['Close'].rolling(window=20).std()
                    symbol_df['BB_Upper'] = symbol_df['BB_Middle'] + (symbol_df['BB_Std'] * 2)
                    symbol_df['BB_Lower'] = symbol_df['BB_Middle'] - (symbol_df['BB_Std'] * 2)
                    
                    # Calculate MACD (Moving Average Convergence Divergence)
                    symbol_df['EMA_12'] = symbol_df['Close'].ewm(span=12, adjust=False).mean()
                    symbol_df['EMA_26'] = symbol_df['Close'].ewm(span=26, adjust=False).mean()
                    symbol_df['MACD'] = symbol_df['EMA_12'] - symbol_df['EMA_26']
                    symbol_df['MACD_Signal'] = symbol_df['MACD'].ewm(span=9, adjust=False).mean()
                    symbol_df['MACD_Histogram'] = symbol_df['MACD'] - symbol_df['MACD_Signal']
                    
                    result_dfs.append(symbol_df)
                
                # Combine all symbol dataframes
                result_df = pd.concat(result_dfs)
                
            # If we have economic indicator data
            elif any(col in df.columns for col in ['GDP_Growth', 'Unemployment_Rate', 'Inflation_Rate']):
                result_df = df.copy()
                
                # Calculate YoY (Year-over-Year) changes for relevant metrics
                # Sort by date first
                result_df.sort_values('Date', inplace=True)
                
                for col in ['GDP_Growth', 'Unemployment_Rate', 'Inflation_Rate', 'Interest_Rate', 'Consumer_Confidence']:
                    if col in result_df.columns:
                        # Calculate YoY change
                        result_df[f'{col}_YoY_Change'] = result_df[col].pct_change(periods=12)  # Assuming monthly data
                
                # Calculate correlation matrix for economic indicators if multiple indicators present
                indicator_cols = [col for col in ['GDP_Growth', 'Unemployment_Rate', 'Inflation_Rate', 
                                                  'Interest_Rate', 'Consumer_Confidence'] 
                                 if col in result_df.columns]
                
                if len(indicator_cols) > 1:
                    corr_matrix = result_df[indicator_cols].corr()
                    logger.info(f"Correlation matrix calculated for {len(indicator_cols)} indicators")
                    
                    # We can't directly add the correlation matrix to the dataframe
                    # But we could save it separately or log it
                    
            else:
                # For any other type of data, just return as is
                result_df = df
                logger.warning("Unknown data format - metrics calculation skipped")
            
            logger.info(f"Calculated metrics for {len(result_df)} rows")
            return result_df
            
        except Exception as e:
            logger.error(f"Error calculating metrics: {str(e)}")
            raise
