# <span style="color:#4AF626">🚀 Financial Market Data ETL Pipeline</span>

![Python](https://img.shields.io/badge/Python-3.8%2B-blue) ![License](https://img.shields.io/badge/License-MIT-green)

<span style="color:#FFFFFF">This project is a comprehensive ETL (Extract, Transform, Load) pipeline for processing financial market data from multiple sources. It extracts data from various financial sources, transforms it to calculate relevant metrics, and loads it into databases and CSV files for analysis.</span>

## <span style="color:#F7FE2E">✨ Quick Links</span>

- [User Guide](USER_GUIDE.md) - Complete instructions for users
- [Developer Guide](DEVELOPER_GUIDE.md) - Extending and customizing the pipeline
- [GitHub Repository](https://github.com/ketankshukla/financial_market_etl)

## <span style="color:#4AF626">📊 Key Features</span>

<span style="color:#FFFFFF">

- ⚡ Extract financial data from multiple sources (CSV, JSON, API)
- 📈 Calculate advanced financial metrics (moving averages, RSI, MACD, Bollinger Bands)
- 🔍 Validate data for consistency and completeness
- 💾 Load processed data into databases or export as CSV files
- 🔄 Extensible architecture for adding new data sources and metrics
- 📝 Comprehensive logging and error handling

</span>

## <span style="color:#F7FE2E">🏗️ Project Structure</span>

<span style="color:#FFFFFF">

- `main.py` - Entry point with command-line argument parsing
- `config.py` - Configuration settings with defaults for paths and database
- `orchestrator.py` - Pipeline execution logic with Task class and SimpleScheduler
- `extractors/` - Modules for different data sources (CSV, JSON, API)
- `transformers/` - Data transformation modules for financial calculations
- `validators/` - Data validation modules
- `loaders/` - Database and CSV export functionality
- `data/` - Sample data and processed outputs
- `logs/` - Execution logs

</span>

## <span style="color:#4AF626">🌐 Data Sources</span>

<span style="color:#FFFFFF">

- 📄 CSV: `./data/stock_prices.csv` (historical stock price data)
- 📊 JSON: `./data/economic_indicators.json` (economic indicators data)
- 🔌 API: Alpha Vantage API (real-time financial data)

</span>

## <span style="color:#F7FE2E">⚙️ Quick Setup</span>

<span style="color:#FFFFFF">

1. Clone the repository:
```powershell
git clone https://github.com/ketankshukla/financial_market_etl.git
cd financial_market_etl
```

2. Set up a virtual environment:
```powershell
python -m venv .venv
./.venv/Scripts/Activate.ps1  # On Windows
source .venv/bin/activate     # On macOS/Linux
```

3. Install dependencies:
```powershell
pip install -r requirements.txt
```

4. Run the ETL pipeline:
```powershell
python main.py --source all
```

</span>

## <span style="color:#4AF626">🧮 Financial Metrics</span>

<span style="color:#FFFFFF">
The pipeline calculates several key financial metrics and indicators:

- Daily Returns
- Moving Averages (20-day and 50-day)
- Moving Average Crossover Signals
- Volatility
- Relative Strength Index (RSI)
- Bollinger Bands
- MACD (Moving Average Convergence Divergence)
</span>

## <span style="color:#F7FE2E">📚 Documentation</span>

<span style="color:#FFFFFF">
For detailed instructions, check the documentation:

- [User Guide](USER_GUIDE.md) - Complete setup and usage instructions
- [Developer Guide](DEVELOPER_GUIDE.md) - Architecture and extension guidelines
</span>

## <span style="color:#4AF626">🤝 Contributing</span>

<span style="color:#FFFFFF">
Contributions are welcome! Please check the Developer Guide for architecture details and best practices before submitting pull requests.
</span>

## <span style="color:#F7FE2E">⚠️ Disclaimer</span>

<span style="color:#FFFFFF">
This project is for educational and demonstration purposes. It is not financial advice, and the calculated metrics should not be the sole basis for investment decisions.
</span>
