#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Orchestrator module for the Financial Market Data ETL pipeline.
Handles task scheduling and execution flow.
"""

import logging
import pandas as pd
from datetime import datetime

# Import extractors
from extractors.csv_extractor import CSVExtractor
from extractors.json_extractor import JSONExtractor
from extractors.api_extractor import APIExtractor

# Import transformers
from transformers.market_data_transformer import MarketDataTransformer
from transformers.metrics_calculator import MetricsCalculator

# Import validators
from validators.data_validator import DataValidator

# Import loaders
from loaders.db_loader import DBLoader
from loaders.csv_loader import CSVLoader

logger = logging.getLogger(__name__)

class Task:
    """Represents a task in the ETL pipeline."""
    
    def __init__(self, name, func, dependencies=None):
        """
        Initialize a Task object.
        
        Args:
            name (str): Task name
            func (callable): Function to execute for this task
            dependencies (list): List of task names this task depends on
        """
        self.name = name
        self.func = func
        self.dependencies = dependencies or []
        self.completed = False
        self.result = None
        
    def execute(self, *args, **kwargs):
        """Execute the task function."""
        logger.info(f"Executing task: {self.name}")
        start_time = datetime.now()
        
        try:
            self.result = self.func(*args, **kwargs)
            self.completed = True
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Task {self.name} completed in {execution_time:.2f} seconds")
            return self.result
        except Exception as e:
            logger.error(f"Task {self.name} failed: {str(e)}")
            raise

class SimpleScheduler:
    """Simple task scheduler for the ETL pipeline."""
    
    def __init__(self):
        """Initialize the scheduler with an empty task dictionary."""
        self.tasks = {}
        
    def add_task(self, task):
        """Add a task to the scheduler."""
        self.tasks[task.name] = task
        
    def run(self, entry_point, *args, **kwargs):
        """
        Run tasks starting from the entry point.
        
        Args:
            entry_point (str): Name of the entry point task
            *args, **kwargs: Arguments to pass to tasks
        
        Returns:
            The result of the entry point task
        """
        if entry_point not in self.tasks:
            raise ValueError(f"Task {entry_point} not found")
        
        task = self.tasks[entry_point]
        
        # Check if dependencies are completed
        for dep_name in task.dependencies:
            if dep_name not in self.tasks:
                raise ValueError(f"Dependency {dep_name} not found")
            
            dep_task = self.tasks[dep_name]
            if not dep_task.completed:
                self.run(dep_name, *args, **kwargs)
        
        # Execute the task
        return task.execute(*args, **kwargs)

class Orchestrator:
    """Orchestrates the ETL pipeline execution."""
    
    def __init__(self, config):
        """
        Initialize the orchestrator.
        
        Args:
            config (Config): Configuration object
        """
        self.config = config
        self.scheduler = SimpleScheduler()
        
        # Initialize components
        self.csv_extractor = CSVExtractor(config)
        self.json_extractor = JSONExtractor(config)
        self.api_extractor = APIExtractor(config)
        
        self.transformer = MarketDataTransformer(config)
        self.metrics_calculator = MetricsCalculator(config)
        
        self.validator = DataValidator(config)
        
        self.db_loader = DBLoader(config)
        self.csv_loader = CSVLoader(config)
        
        # Set up tasks
        self._setup_tasks()
        
    def _setup_tasks(self):
        """Set up tasks for the ETL pipeline."""
        # Extract tasks - define wrapper functions to handle parameters correctly
        self.scheduler.add_task(Task("extract_csv", lambda *args, **kwargs: self.csv_extractor.extract()))
        self.scheduler.add_task(Task("extract_json", lambda *args, **kwargs: self.json_extractor.extract()))
        self.scheduler.add_task(Task("extract_api", lambda *args, **kwargs: 
                               self.api_extractor.extract(**{k: kwargs[k] for k in ['symbols', 'start_date', 'end_date'] 
                                                            if k in kwargs})))
        
        # Transform tasks - use wrapper functions to handle parameters correctly
        self.scheduler.add_task(Task("transform_csv_data", 
                                    lambda *args, **kwargs: self.transformer.transform_csv_data(args[0] if args else None), 
                                    ["extract_csv"]))
        self.scheduler.add_task(Task("transform_json_data", 
                                    lambda *args, **kwargs: self.transformer.transform_json_data(args[0] if args else None), 
                                    ["extract_json"]))
        self.scheduler.add_task(Task("transform_api_data", 
                                    lambda *args, **kwargs: self.transformer.transform_api_data(args[0] if args else None), 
                                    ["extract_api"]))
        
        # Calculate metrics - use a wrapper to handle multiple dataframes
        self.scheduler.add_task(Task("calculate_metrics", 
                                    lambda *args, **kwargs: 
                                        self.metrics_calculator.calculate(**{k: kwargs[k] for k in kwargs if k in ['csv_data', 'json_data', 'api_data']}), 
                                    ["transform_csv_data", "transform_json_data", "transform_api_data"]))
        
        # Validate tasks
        self.scheduler.add_task(Task("validate_data", 
                                    self.validator.validate, 
                                    ["calculate_metrics"]))
        
        # Load tasks
        self.scheduler.add_task(Task("load_to_db", 
                                    self.db_loader.load, 
                                    ["validate_data"]))
        self.scheduler.add_task(Task("export_to_csv", 
                                    self.csv_loader.export, 
                                    ["validate_data"]))
    
    def run_etl(self, source, stock_symbols=None, start_date=None, end_date=None):
        """
        Run the ETL pipeline for the specified source.
        
        Args:
            source (str): Data source ('csv', 'json', or 'api')
            stock_symbols (str, optional): Comma-separated list of stock symbols
            start_date (str, optional): Start date for historical data
            end_date (str, optional): End date for historical data
        """
        logger.info(f"Running ETL pipeline for {source} source")
        
        # Parse stock symbols
        symbols = self.config.default_symbols
        if stock_symbols:
            symbols = stock_symbols.split(',')
        
        # Set dates
        start = start_date or self.config.default_start_date
        end = end_date or self.config.default_end_date
        
        # Run appropriate tasks based on source
        csv_data = pd.DataFrame()
        json_data = pd.DataFrame()
        api_data = pd.DataFrame()
        
        # Process each source individually to avoid parameter conflicts
        if source == 'csv' or source == 'all':
            # For CSV source, don't pass any parameters that the extract_csv task doesn't expect
            csv_raw = self.scheduler.run("extract_csv")
            csv_data = self.scheduler.run("transform_csv_data", csv_raw)
        
        if source == 'json' or source == 'all':
            # For JSON source, don't pass any parameters that the extract_json task doesn't expect
            json_raw = self.scheduler.run("extract_json")
            json_data = self.scheduler.run("transform_json_data", json_raw)
        
        if source == 'api' or source == 'all':
            # For API source, only pass the parameters the extract_api task expects
            api_raw = self.scheduler.run("extract_api", symbols=symbols, start_date=start, end_date=end)
            api_data = self.scheduler.run("transform_api_data", api_raw)
        
        # Calculate metrics with all available data
        metrics_data = self.scheduler.run("calculate_metrics", 
                                        csv_data=csv_data, 
                                        json_data=json_data, 
                                        api_data=api_data)
        
        # Only proceed if we have data
        if not metrics_data.empty:
            validated_data = self.scheduler.run("validate_data", metrics_data)
            self.scheduler.run("load_to_db", validated_data)
            self.scheduler.run("export_to_csv", validated_data)
        
        logger.info(f"ETL pipeline for {source} source completed successfully")
