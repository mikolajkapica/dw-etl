"""
Simplified resources for the Himalayan Expeditions ETL pipeline.
Resources provide database connections, file system access, and configuration.
"""

from dataclasses import dataclass
import os
import logging
from sqlalchemy import create_engine, text
import pandas as pd
from dagster import RetryPolicy, ConfigurableResource


class DatabaseResource:
    def __init__(
        self,
        server: str = None,
        database: str = None,
        username: str = None,
        password: str = None,
        driver: str = None,
    ):
        self.server = server or os.getenv("DB_SERVER", "localhost")
        self.database = database or os.getenv("DB_NAME", "HimalayanExpeditionsDW")
        self.username = username or os.getenv("DB_USERNAME", "")
        self.password = password or os.getenv("DB_PASSWORD", "")
        self.driver = driver or os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
        self._engine = None

    def get_connection_string(self) -> str:
        return (
            f"mssql+pyodbc://@{self.server}/{self.database}"
            f"?driver={self.driver}&trusted_connection=yes"
            f"&TrustServerCertificate=yes&Encrypt=yes"
        )

    def get_engine(self):
        if self._engine is None:
            connection_string = self.get_connection_string()
            self._engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
            )
        return self._engine

    def execute_query(self, query: str, params: dict[str, any] = None) -> pd.DataFrame:
        try:
            with self.get_engine().connect() as conn:
                return pd.read_sql(text(query), conn, params=params)
        except Exception as e:
            logging.error(f"Database query failed: {e}")
            raise
    def bulk_insert(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> int:
        """Bulk insert DataFrame into database table."""
        if df.empty:
            logging.warning(f"No data to insert into {table_name}")
            return 0

        try:
            rows_affected = df.to_sql(
                table_name,
                self.get_engine(),
                if_exists="fail",
                index=False,
                method="multi",
                chunksize=100,
            )
            logging.info(f"Successfully inserted {len(df)} rows into {table_name}")
            return len(df)
        except Exception as e:
            logging.error(f"Bulk insert failed for {table_name}: {e}")
            raise
        
    def upsert_dimension(self, df: pd.DataFrame, table_name: str, key_columns: List[str], update_columns: List[str] = None) -> int:
        """
        Perform upsert operation on dimension table.
        
        Args:
            df: DataFrame with data to upsert
            table_name: Target table name
            key_columns: Columns used to identify existing records
            update_columns: Columns to update (if None, all non-key columns)
            
        Returns:
            Number of rows affected
        """
        if df.empty:
            return 0
            
        if update_columns is None:
            update_columns = [col for col in df.columns if col not in key_columns]
        
        try:
            with self.get_engine().connect() as conn:
                # For simplicity, we'll use a basic insert strategy with conflict handling
                # In a production environment, you might want to implement proper MERGE statements
                
                # First, try to insert all records
                # This will work for new records
                try:
                    # Exclude key columns (e.g., identity columns) from insert
                    insert_columns = [col for col in df.columns if col not in key_columns]
                    df_insert = df[insert_columns]
                    rows_inserted = df_insert.to_sql(
                        table_name, 
                        conn, 
                        if_exists='append', 
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    logging.info(f"Inserted {len(df_insert)} new rows into {table_name}")
                    return len(df_insert)
                except Exception as insert_error:
                    # If insert fails due to duplicates, we need to handle updates
                    logging.warning(f"Bulk insert failed, trying upsert approach: {insert_error}")
                    
                    # For each row, try insert first, then update if it fails
                    rows_affected = 0
                    for _, row in df.iterrows():
                        try:
                            # Try insert first, excluding key columns
                            row_insert = {col: row[col] for col in insert_columns}
                            row_df = pd.DataFrame([row_insert])
                            row_df.to_sql(table_name, conn, if_exists='append', index=False)
                            rows_affected += 1
                        except:  # If insert fails, try update
                            try:
                                # Build update statement with named parameters
                                set_clause = ", ".join([f"{col} = :upd_{col}" for col in update_columns])
                                where_clause = " AND ".join([f"{col} = :key_{col}" for col in key_columns])
                                
                                update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                                
                                # Prepare parameters as dictionary
                                params = {}
                                for col in update_columns:
                                    params[f"upd_{col}"] = row[col]
                                for col in key_columns:
                                    params[f"key_{col}"] = row[col]
                                
                                result = conn.execute(text(update_sql), params)
                                if result.rowcount > 0:
                                    rows_affected += 1
                            except Exception as update_error:
                                logging.error(f"Failed to update row: {update_error}")
                                continue
                    
                    return rows_affected
                    
        except Exception as e:
            logging.error(f"Upsert operation failed for {table_name}: {e}")
            raise


class FileSystemResource:
    def __init__(self, base_path: str = None):
        self.base_path = base_path or os.getenv("DATA_SOURCE_PATH", "data/")

    def get_file_path(self, filename: str) -> str:
        return os.path.join(self.base_path, filename)

    def file_exists(self, filename: str) -> bool:
        return os.path.exists(self.get_file_path(filename))

    def read_csv(self, filename: str, **kwargs) -> pd.DataFrame:
        file_path = self.get_file_path(filename)
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Data file not found: {file_path}")
            df = pd.read_csv(file_path, **kwargs)
            logging.info(f"Successfully loaded {len(df)} rows from {filename}")
            return df
        except Exception as e:
            raise Exception(f"Failed to read {filename}: {e}")


@dataclass
class WorldBankConfig(ConfigurableResource):
    base_url: str
    start_year: int
    end_year: int
    indicators: list[str]
    timeout: int = 10
    max_page_size: int = 32768


@dataclass
class ETLConfigResource:
    data_directory: str
    log_level: str
