# 📊 Financial Market ETL - User Guide

## <span style="color:#4AF626">🚀 Introduction</span>

Welcome to the Financial Market ETL pipeline! This tool allows you to process financial market data from multiple sources (CSV files, JSON files, and APIs), transform the data with various financial metrics, and load the results into a database or export to CSV files.

## <span style="color:#F7FE2E">🛠️ Prerequisites</span>

Before running the ETL pipeline, ensure you have:

- **Python 3.8+** installed on your system
- Git installed (if you want to clone the repository)
- Basic understanding of financial market data

## <span style="color:#4AF626">⚙️ Installation & Setup</span>

### 1. Get the Code

Either clone the repository or download it directly:

```powershell
git clone https://github.com/ketankshukla/financial_market_etl.git
cd financial_market_etl
```

### 2. Set Up a Virtual Environment

Using a virtual environment is recommended to isolate the project dependencies:

**Windows:**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**macOS/Linux:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

Once your virtual environment is activated, install the required packages:

```powershell
pip install -r requirements.txt
```

### 4. Prepare Data Sources

The ETL pipeline comes with sample data for demonstration:
- `data/stock_prices.csv` - Sample historical stock price data 
- `data/economic_indicators.json` - Sample economic indicator data

If these files don't exist, the pipeline will automatically create sample data for demonstration purposes.

### 5. API Configuration (Optional)

For live financial data, obtain an API key from Alpha Vantage:
1. Visit [Alpha Vantage](https://www.alphavantage.co/support/#api-key)
2. Register for a free API key
3. Add your API key to `config.py` or set it as an environment variable:

```powershell
# Windows PowerShell
$env:ALPHA_VANTAGE_API_KEY="YOUR_API_KEY_HERE"

# macOS/Linux
export ALPHA_VANTAGE_API_KEY="YOUR_API_KEY_HERE"
```

Without an API key, the pipeline will use mock data for demonstration.

## <span style="color:#F7FE2E">▶️ Running the Pipeline</span>

### Basic Usage

To run the ETL pipeline with all data sources:

```powershell
python main.py
```

### Processing Specific Data Sources

To process only specific data sources:

```powershell
python main.py --source csv     # Process only CSV data
python main.py --source json    # Process only JSON data
python main.py --source api     # Process only API data
```

### Additional Options

Filter by stock symbols:
```powershell
python main.py --symbols AAPL,MSFT,GOOGL
```

Specify a date range:
```powershell
python main.py --start-date 2024-01-01 --end-date 2024-12-31
```

## <span style="color:#4AF626">🔍 Understanding the Results</span>

After running the pipeline, you'll find:

### 1. Console Output

The pipeline provides detailed logging in the console, showing:
- Data extraction progress
- Transformation steps
- Validation warnings
- Database loading status
- CSV export paths

### 2. Database Results

Data is loaded into an SQLite database at `data/financial_market.db`.

You can query this with any SQLite tool or with Python:

```python
import sqlite3

# Connect to the database
conn = sqlite3.connect('data/financial_market.db')
cursor = conn.cursor()

# Example query
cursor.execute("SELECT Symbol, AVG(Close) as avg_price FROM stock_prices GROUP BY Symbol")
results = cursor.fetchall()
print(results)

# Close connection
conn.close()
```

### 3. CSV Outputs

Processed data is exported to CSV files in the `data/processed/` directory:
- Individual files per stock symbol
- A consolidated file with all data

Files are named with timestamps for easy tracking.

## <span style="color:#F7FE2E">🔄 Financial Metrics</span>

The pipeline calculates several useful financial metrics:

- **Daily Returns**: Day-to-day percentage change
- **Moving Averages**: Short (20-day) and long (50-day) moving averages
- **Moving Average Signal**: Buy/sell signals based on moving average crossovers
- **Volatility**: Based on standard deviation of returns
- **Bollinger Bands**: Middle band (20-day moving average) and upper/lower bands
- **RSI (Relative Strength Index)**: Momentum indicator
- **MACD (Moving Average Convergence Divergence)**: Trend-following momentum indicator

## <span style="color:#4AF626">❓ Troubleshooting</span>

### Common Issues

1. **Missing Dependencies**
   - Ensure all packages are installed: `pip install -r requirements.txt`
   - For compilation errors, try an older version of pandas: `pip install pandas==1.5.3`

2. **Data Source Issues**
   - CSV/JSON files not found: Place sample files in the `data/` directory
   - API errors: Verify your API key in config.py or environment variables

3. **Database Errors**
   - Ensure the `data/` directory exists
   - Check write permissions for the database file

### Logging

All pipeline runs create detailed log files in the `logs/` directory.
Review these logs for detailed debugging information.

## <span style="color:#F7FE2E">🔒 Security Considerations</span>

- Never commit API keys to version control
- Use environment variables for sensitive information
- The default database is SQLite; for production, consider a more robust database system

## <span style="color:#4AF626">🔗 Additional Resources</span>

- [Developer Guide](DEVELOPER_GUIDE.md) - For extending and customizing the pipeline
- [GitHub Repository](https://github.com/ketankshukla/financial_market_etl)
- [Alpha Vantage Documentation](https://www.alphavantage.co/documentation/)
