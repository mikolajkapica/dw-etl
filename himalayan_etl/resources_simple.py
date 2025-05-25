"""
Simplified resources for the Himalayan Expeditions ETL pipeline.
Resources provide database connections, file system access, and configuration.
"""

import os
import logging
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text
import pandas as pd
from dagster import resource, InitResourceContext, ConfigurableResource
from pathlib import Path


class DatabaseResource:
    """SQL Server database resource for ETL operations."""
    
    def __init__(
        self,
        server: str = None,
        database: str = None,
        username: str = None,
        password: str = None,
        driver: str = None,
        trusted_connection: bool = False
    ):
        self.server = server or os.getenv("DB_SERVER", "localhost")
        self.database = database or os.getenv("DB_NAME", "HimalayanExpeditionsDW")
        self.username = username or os.getenv("DB_USERNAME", "")
        self.password = password or os.getenv("DB_PASSWORD", "")
        self.driver = driver or os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
        self.trusted_connection = trusted_connection
        self._engine = None
    
    def get_connection_string(self) -> str:
        """Build SQL Server connection string."""
        if self.trusted_connection:
            return (f"mssql+pyodbc://@{self.server}/{self.database}"
                   f"?driver={self.driver}&trusted_connection=yes")
        else:
            return (f"mssql+pyodbc://{self.username}:{self.password}@"
                   f"{self.server}/{self.database}?driver={self.driver}")
    
    def get_engine(self):
        """Get SQLAlchemy engine with connection pooling."""
        if self._engine is None:
            connection_string = self.get_connection_string()
            self._engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600
            )
        return self._engine
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        try:
            with self.get_engine().connect() as conn:
                return pd.read_sql(text(query), conn, params=params)
        except Exception as e:
            logging.error(f"Database query failed: {e}")
            raise
    
    def bulk_insert(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> int:
        """Bulk insert DataFrame into database table."""
        try:
            rows_affected = df.to_sql(
                table_name, 
                self.get_engine(), 
                if_exists=if_exists, 
                index=False,
                method='multi',
                chunksize=1000
            )
            logging.info(f"Inserted {len(df)} rows into {table_name}")
            return len(df)
        except Exception as e:
            logging.error(f"Bulk insert failed for {table_name}: {e}")
            raise


class FileSystemResource:
    """File system resource for reading source data files."""
    
    def __init__(self, base_path: str = None):
        self.base_path = base_path or os.getenv("DATA_SOURCE_PATH", "data/")
    
    def get_file_path(self, filename: str) -> str:
        """Get full path for a data file."""
        return os.path.join(self.base_path, filename)
    
    def read_csv(self, filename: str, **kwargs) -> pd.DataFrame:
        """Read CSV file with error handling."""
        file_path = self.get_file_path(filename)
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Data file not found: {file_path}")
            
            df = pd.read_csv(file_path, **kwargs)
            logging.info(f"Successfully loaded {len(df)} rows from {filename}")
            return df
        except Exception as e:
            logging.error(f"Failed to read {filename}: {e}")
            raise
    
    def file_exists(self, filename: str) -> bool:
        """Check if a file exists."""
        return os.path.exists(self.get_file_path(filename))


class ETLConfigResource:
    """ETL configuration resource with data mappings and settings."""
    
    def __init__(
        self,
        batch_size: int = None,
        max_retry_attempts: int = None,
        log_level: str = None,
        data_quality_threshold: float = 0.8,
        data_directory: str = None,
        output_directory: str = None,
        world_bank_base_url: str = None
    ):
        self.batch_size = batch_size or int(os.getenv("BATCH_SIZE", "1000"))
        self.max_retry_attempts = max_retry_attempts or int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
        self.log_level = log_level or os.getenv("LOG_LEVEL", "INFO")
        self.data_quality_threshold = data_quality_threshold
        self.data_directory = data_directory or "./data"
        self.output_directory = output_directory or "./output"
        self.world_bank_base_url = world_bank_base_url or "https://api.worldbank.org/v2"
    
    @property
    def season_mapping(self) -> Dict[str, str]:
        """Season code standardization mapping."""
        return {
            "SPRING": "Spring",
            "SUMMER": "Summer", 
            "AUTUMN": "Autumn",
            "WINTER": "Winter",
            "FALL": "Autumn",
            "PRE-MONSOON": "Spring",
            "POST-MONSOON": "Autumn",
            "MONSOON": "Summer",
        }
    
    @property
    def termination_reason_mapping(self) -> Dict[str, Dict[str, Any]]:
        """Termination reason categorization mapping."""
        return {
            "SUCCESS (MAIN PEAK)": {"category": "Success", "is_success": True},
            "SUCCESS (SUBPEAK)": {"category": "Success", "is_success": True},
            "UNSUCCESSFUL (ATTEMPT)": {"category": "Attempt", "is_success": False},
            "UNSUCCESSFUL (OTHER)": {"category": "Failed", "is_success": False},
            "ABANDONED": {"category": "Abandoned", "is_success": False},
            "ACCIDENT": {"category": "Accident", "is_success": False},
            "ILLNESS": {"category": "Medical", "is_success": False},
            "WEATHER": {"category": "Weather", "is_success": False},
            "ROUTE": {"category": "Route", "is_success": False},
            "PERMITS": {"category": "Administrative", "is_success": False},
            "UNKNOWN": {"category": "Unknown", "is_success": False},
        }
    
    @property
    def country_code_mapping(self) -> Dict[str, str]:
        """Country name to ISO3 code mapping."""
        return {
            "NEPAL": "NPL",
            "INDIA": "IND", 
            "PAKISTAN": "PAK",
            "CHINA": "CHN",
            "TIBET": "CHN",  # Tibet is part of China
            "BHUTAN": "BTN",
            "MYANMAR": "MMR",
            "BURMA": "MMR",  # Historical name
            "USA": "USA",
            "UNITED STATES": "USA",
            "UK": "GBR",
            "UNITED KINGDOM": "GBR",
            "GREAT BRITAIN": "GBR",
            "RUSSIA": "RUS",
            "USSR": "RUS",  # Historical
            "SOVIET UNION": "RUS",  # Historical
            "JAPAN": "JPN",
            "SOUTH KOREA": "KOR",
            "KOREA": "KOR",
        }


# Simple resource factory functions
def create_database_resource(**kwargs) -> DatabaseResource:
    """Create database resource with configuration."""
    return DatabaseResource(**kwargs)


def create_filesystem_resource(**kwargs) -> FileSystemResource:
    """Create filesystem resource with configuration."""
    return FileSystemResource(**kwargs)


def create_etl_config_resource(**kwargs) -> ETLConfigResource:
    """Create ETL configuration resource."""
    return ETLConfigResource(**kwargs)
