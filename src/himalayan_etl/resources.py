"""
Dagster resources for Himalayan Expeditions ETL pipeline.

This module defines resources for database connections, file system access,
and other external dependencies used throughout the ETL process.
"""

import os
from typing import Any, Dict, Optional
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dagster import ConfigurableResource, InitResourceContext
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DatabaseResource(ConfigurableResource):
    """
    SQL Server database resource for ETL operations.
    
    Provides connection management, transaction handling, and bulk operations
    for loading data into the data warehouse.
    """
    
    server: str = os.getenv("DB_SERVER", "localhost")
    database: str = os.getenv("DB_NAME", "HimalayanExpeditionsDW")
    username: str = os.getenv("DB_USERNAME", "")
    password: str = os.getenv("DB_PASSWORD", "")
    driver: str = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    trusted_connection: bool = False
    
    def get_connection_string(self) -> str:
        """Generate SQL Server connection string."""
        if self.trusted_connection:
            return (
                f"mssql+pyodbc://@{self.server}/{self.database}"
                f"?driver={self.driver.replace(' ', '+')}&trusted_connection=yes"
            )
        else:
            return (
                f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}"
                f"?driver={self.driver.replace(' ', '+')}"
            )
    
    def get_engine(self) -> Engine:
        """Create SQLAlchemy engine."""
        connection_string = self.get_connection_string()
        return create_engine(
            connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SELECT query and return results as DataFrame."""
        engine = self.get_engine()
        return pd.read_sql_query(query, engine, params=params)
    
    def execute_non_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """Execute a non-SELECT query and return affected rows count."""
        engine = self.get_engine()
        with engine.begin() as conn:
            result = conn.execute(text(query), params or {})
            return result.rowcount
    
    def bulk_insert(self, df: pd.DataFrame, table_name: str, 
                   if_exists: str = "append", method: str = "multi") -> int:
        """
        Bulk insert DataFrame into SQL Server table.
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            method: Insertion method ('multi' for bulk insert)
            
        Returns:
            Number of rows inserted
        """
        engine = self.get_engine()
        
        # Handle NaN values that cause issues with SQL Server
        df_clean = df.copy()
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].fillna('')
            else:
                df_clean[col] = df_clean[col].fillna(0)
        
        rows_inserted = df_clean.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method=method,
            chunksize=1000
        )
        
        return len(df_clean) if rows_inserted is None else rows_inserted
    
    def upsert_dimension(self, df: pd.DataFrame, table_name: str, 
                        key_columns: list, update_columns: list) -> int:
        """
        Perform upsert (insert or update) operation for dimension tables.
        
        Args:
            df: DataFrame with data to upsert
            table_name: Target dimension table
            key_columns: Columns used to match existing records
            update_columns: Columns to update if record exists
            
        Returns:
            Number of rows affected
        """
        if df.empty:
            return 0
            
        engine = self.get_engine()
        temp_table = f"#{table_name}_temp"
        
        with engine.begin() as conn:
            # Create temporary table and load data
            df.to_sql(temp_table, conn, if_exists="replace", index=False)
            
            # Build MERGE statement
            key_conditions = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
            update_sets = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
            insert_columns = ", ".join(df.columns)
            insert_values = ", ".join([f"source.{col}" for col in df.columns])
            
            merge_sql = f"""
            MERGE {table_name} AS target
            USING {temp_table} AS source
            ON {key_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_sets}, ModifiedDate = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
            """
            
            result = conn.execute(text(merge_sql))
            return result.rowcount


class FileSystemResource(ConfigurableResource):
    """
    File system resource for accessing CSV and other data files.
    
    Provides standardized file access patterns and error handling
    for reading source data files.
    """
    
    base_path: str = os.getenv("DATA_SOURCE_PATH", "data/")
    
    def get_file_path(self, filename: str) -> str:
        """Get full path for a data file."""
        return os.path.join(self.base_path, filename)
    
    def file_exists(self, filename: str) -> bool:
        """Check if a data file exists."""
        return os.path.exists(self.get_file_path(filename))
    
    def read_csv(self, filename: str, **kwargs) -> pd.DataFrame:
        """
        Read CSV file with standardized error handling.
        
        Args:
            filename: Name of CSV file to read
            **kwargs: Additional arguments for pd.read_csv
            
        Returns:
            DataFrame with file contents
        """
        file_path = self.get_file_path(filename)
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file not found: {file_path}")
        
        # Default CSV reading parameters
        default_params = {
            'encoding': 'utf-8',
            'low_memory': False,
            'na_values': ['', 'NULL', 'null', 'NA', 'N/A', '#N/A', 'NaN'],
            'keep_default_na': True,
        }
        default_params.update(kwargs)
        
        try:
            df = pd.read_csv(file_path, **default_params)
            return df
        except UnicodeDecodeError:
            # Try different encoding if UTF-8 fails
            default_params['encoding'] = 'latin-1'
            df = pd.read_csv(file_path, **default_params)
            return df
    
    def get_file_info(self, filename: str) -> Dict[str, Any]:
        """Get metadata about a data file."""
        file_path = self.get_file_path(filename)
        
        if not os.path.exists(file_path):
            return {"exists": False}
        
        stat = os.stat(file_path)
        return {
            "exists": True,
            "size_bytes": stat.st_size,
            "modified_time": stat.st_mtime,
            "full_path": file_path,
        }


class ETLConfigResource(ConfigurableResource):
    """
    Configuration resource for ETL pipeline settings.
    
    Centralizes configuration parameters used across multiple ops
    and provides validation for configuration values.
    """
    
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    max_retry_attempts: int = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    data_quality_threshold: float = float(os.getenv("DATA_QUALITY_THRESHOLD", "0.8"))
    
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


# Resource instances for use in Dagster definitions
database_resource = DatabaseResource()
filesystem_resource = FileSystemResource()
etl_config_resource = ETLConfigResource()
