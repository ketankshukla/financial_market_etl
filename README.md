# Financial Market Data ETL Pipeline

This project is a data pipeline for processing financial market data from multiple sources (CSV, JSON, API). It extracts data from various financial sources, transforms it to calculate relevant metrics, and loads it into databases for analysis.

## Project Structure
- `main.py` - Entry point with command-line argument parsing
- `config.py` - Configuration settings with defaults for paths and database
- `orchestrator.py` - Pipeline execution logic with Task class and SimpleScheduler
- `extractors/` - Modules for different data sources (CSV, JSON, API)
- `transformers/` - Data transformation modules for financial calculations
- `validators/` - Data validation
- `loaders/` - Database and CSV export functionality

## Data Sources
- CSV: ./data/stock_prices.csv (historical stock price data)
- JSON: ./data/economic_indicators.json (economic indicators data)
- API: Alpha Vantage and Yahoo Finance APIs (real-time financial data)

## Setup and Usage
1. Install dependencies:
```
pip install -r requirements.txt
```

2. Run the ETL pipeline:
```
python main.py --source all
```

Available source options: csv, json, api, all

## Features
- Extract financial data from multiple sources
- Calculate financial metrics (moving averages, volatility, etc.)
- Validate data for consistency and completeness
- Load processed data into databases or export as CSV
