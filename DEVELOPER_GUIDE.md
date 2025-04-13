# 🧠 Financial Market ETL - Developer Guide

## <span style="color:#4AF626">🏗️ Project Architecture</span>

The Financial Market ETL pipeline follows a modular architecture with clear separation of concerns:

```
financial_market_etl/
├── data/                    # Data storage
│   ├── processed/           # Processed output files
│   ├── economic_indicators.json  # Sample JSON data
│   └── stock_prices.csv     # Sample CSV data
├── extractors/              # Data extraction modules
├── transformers/            # Data transformation modules
├── validators/              # Data validation modules
├── loaders/                 # Data loading modules
├── logs/                    # Log files
├── config.py                # Configuration settings
├── main.py                  # Entry point
├── orchestrator.py          # Pipeline control flow
└── requirements.txt         # Dependencies
```

## <span style="color:#F7FE2E">🔄 ETL Pipeline Flow</span>

The pipeline follows this process flow:

1. **Extract**: Data is extracted from various sources (CSV, JSON, API)
2. **Transform**: Raw data is transformed and enriched with financial metrics
3. **Validate**: Data quality checks are performed
4. **Load**: Processed data is loaded into a database and/or exported to CSV files

## <span style="color:#4AF626">📂 Core Components</span>

### 1. Configuration (`config.py`)

The `Config` class centralizes all configuration parameters:

- File paths
- Database settings
- API credentials
- Default parameters
- Transformation and validation thresholds

To extend the config:
```python
# Add new configuration parameters
def __init__(self):
    # Existing config...
    
    # Add your new parameters
    self.new_parameter = value
```

### 2. Orchestrator (`orchestrator.py`)

Controls the execution flow using a simple task scheduler:

- `Task`: Represents a single ETL operation
- `SimpleScheduler`: Manages task execution based on dependencies
- `Orchestrator`: Sets up and runs the ETL workflow

The orchestrator uses lambda wrappers to handle parameter passing between tasks.

### 3. Extractors (`extractors/`)

Each data source has its own extractor module:

- `CSVExtractor`: Extracts data from CSV files
- `JSONExtractor`: Extracts data from JSON files
- `APIExtractor`: Extracts data from financial APIs

To add a new extractor:
1. Create a new Python module in `extractors/`
2. Implement a class with an `extract()` method
3. Register the extractor in `orchestrator.py`

Example:
```python
# extractors/new_extractor.py
class NewExtractor:
    def __init__(self, config):
        self.config = config
        
    def extract(self, *args, **kwargs):
        # Extract data...
        return data_frame
```

### 4. Transformers (`transformers/`)

Transformers process and enrich the data:

- `MarketDataTransformer`: Transforms data from different sources
- `MetricsCalculator`: Calculates financial metrics and indicators

To add new transformations:
1. Extend an existing transformer or create a new one
2. Implement transformation logic
3. Update the task dependencies in `orchestrator.py`

### 5. Validators (`validators/`)

Validators ensure data quality and integrity:

- `DataValidator`: Validates data against quality rules

To add new validation rules:
1. Extend the `validate()` method in `DataValidator`
2. Implement your validation logic
3. Add appropriate logging and warnings

### 6. Loaders (`loaders/`)

Loaders store processed data:

- `DBLoader`: Loads data into an SQLite database
- `CSVLoader`: Exports data to CSV files

To add a new loader:
1. Create a new module in `loaders/`
2. Implement a class with a `load()` method
3. Register in `orchestrator.py`

## <span style="color:#F7FE2E">🧪 Testing</span>

The project includes unit tests in `test_etl.py`. Tests cover:

- Data extraction from each source
- Transformation logic
- Validation rules
- Loading functionality

To run tests:
```powershell
python test_etl.py
```

To add new tests:
1. Extend the `TestFinancialMarketETL` class
2. Add test methods prefixed with `test_`
3. Use appropriate assertions to verify behavior

## <span style="color:#4AF626">🔌 Extending the Pipeline</span>

### Adding a New Data Source

1. Create a new extractor in `extractors/`
2. Implement the `extract()` method
3. Add the extractor to `orchestrator.py`:
   ```python
   # In Orchestrator.__init__
   self.new_extractor = NewExtractor(config)
   
   # In Orchestrator._setup_tasks
   self.scheduler.add_task(Task("extract_new", 
       lambda *args, **kwargs: self.new_extractor.extract()))
   ```
4. Add transformation logic for the new data source
5. Update the ETL flow in `run_etl()` method

### Adding New Financial Metrics

1. Extend `MetricsCalculator` in `transformers/metrics_calculator.py`:
   ```python
   def calculate(self, csv_data=None, json_data=None, api_data=None):
       # Existing code...
       
       # Add your new metric calculation
       df['new_metric'] = self._calculate_new_metric(df)
       
       return df
   
   def _calculate_new_metric(self, df):
       # Implement your metric calculation logic
       return result
   ```

### Adding a New Output Format

1. Create a new loader in `loaders/`
2. Implement the required functionality
3. Add to `orchestrator.py` similar to other loaders

## <span style="color:#F7FE2E">📦 Dependency Management</span>

Dependencies are specified in `requirements.txt`. 

When adding new dependencies:
1. Add them to `requirements.txt`
2. Specify a version range instead of exact version where possible
3. Document any special installation requirements

## <span style="color:#4AF626">🔄 Parameter Handling</span>

The pipeline uses lambda wrappers to handle parameters correctly between tasks. 
When modifying these wrappers, ensure:

1. Proper parameter filtering for each function
2. Default values for optional parameters
3. Consistent error handling

Example:
```python
lambda *args, **kwargs: self.function(**{
    k: kwargs[k] for k in ['param1', 'param2'] if k in kwargs
})
```

## <span style="color:#F7FE2E">📊 Data Schemas</span>

### Stock Price Data

Main columns:
- `Date`: Trading date
- `Symbol`: Stock ticker symbol
- `Open`, `High`, `Low`, `Close`: Price data
- `Volume`: Trading volume
- `Source`: Data source identifier

### Economic Indicator Data

Main columns:
- `date`: Date of the indicator value
- `indicator`: Name of the economic indicator
- `value`: Numeric value
- `unit`: Unit of measurement
- `frequency`: Frequency of updates

### Financial Metrics

Generated metrics include:
- `Daily_Return`: Percentage change in closing price
- `MA_20`, `MA_50`: 20 and 50-day moving averages
- `MA_Signal`: Signal based on moving average crossovers
- `Volatility`: Price volatility measure
- `RSI`: Relative Strength Index (0-100)
- `BB_Middle`, `BB_Upper`, `BB_Lower`: Bollinger Bands
- `MACD`, `MACD_Signal`, `MACD_Histogram`: MACD components

## <span style="color:#4AF626">🔍 Logging Strategy</span>

The project uses Python's built-in logging module:

- `INFO` level for normal operation
- `WARNING` for potential issues
- `ERROR` for operation failures
- `DEBUG` for detailed troubleshooting

To extend logging:
```python
import logging
logger = logging.getLogger(__name__)

# In your function
logger.info("Informational message")
logger.warning("Warning message")
logger.error("Error message")
logger.debug("Debug information")
```

## <span style="color:#F7FE2E">🛡️ Best Practices</span>

1. **Error Handling**: Use try/except blocks to gracefully handle exceptions
2. **Parameter Validation**: Validate inputs early
3. **Data Integrity**: Use validators to maintain data quality
4. **Configurability**: Add new parameters to `config.py`
5. **Testability**: Write unit tests for new features
6. **Documentation**: Update comments and docstrings

## <span style="color:#4AF626">📚 Contribution Guidelines</span>

1. Fork the repository
2. Create a feature branch
3. Add your changes
4. Write/update tests
5. Run tests
6. Submit a pull request

## <span style="color:#F7FE2E">🔗 Resources</span>

- [User Guide](USER_GUIDE.md) - For end users
- [pandas Documentation](https://pandas.pydata.org/docs/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Alpha Vantage API](https://www.alphavantage.co/documentation/)
