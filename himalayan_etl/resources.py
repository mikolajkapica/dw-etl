"""
Simplified resources for the Himalayan Expeditions ETL pipeline.
Resources provide database connections, file system access, and configuration.
"""

from dataclasses import dataclass
import os
import logging
import time
from sqlalchemy import create_engine, text
import pandas as pd
from dagster import OpExecutionContext, RetryPolicy, ConfigurableResource


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

    def execute_query(self, context: OpExecutionContext, query: str, params: dict[str, any] = None) -> pd.DataFrame:
        try:
            with self.get_engine().connect() as conn:
                return pd.read_sql(text(query), conn, params=params)
        except Exception as e:
            context.log.error(f"Database query failed: {e}")
            raise

    def bulk_insert(self, context: OpExecutionContext, df: pd.DataFrame, table_name: str) -> int:
        if df.empty:
            context.log.warning(f"No data to insert into {table_name}")
            return 0

        context.log.info(f"Starting bulk insert into {table_name} with {len(df)} records")

        try:
            rows_affected = df.to_sql(
                table_name,
                self.get_engine(),
                if_exists="append",
                index=False,
                method="multi",
                chunksize=50,
            )
            context.log.info(f"Successfully inserted {len(df)} rows into {table_name}")
            return len(df)
        except Exception as e:
            context.log.error(f"Bulk insert failed for {table_name}: {e}")
            raise
    
    def table_exists(self, context: OpExecutionContext, table_name: str) -> bool:
        query = f"""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = :table_name
        """
        result = self.execute_query(context, query, {"table_name": table_name})
        return result.iloc[0, 0] > 0
    
    def drop_table(self, context: OpExecutionContext, table_name: str):
        query = f"DROP TABLE IF EXISTS {table_name}"
        try:
            context.log.info(f"Executing drop table query: {query}")
            with self.get_engine().connect() as conn:
                conn.execute(text(query))
                conn.commit()
            context.log.info(f"Successfully dropped table {table_name}")
        except Exception as e:
            context.log.error(f"Failed to drop table {table_name}: {e}")
            raise Exception(f"Failed to drop table {table_name}: {e}")
    
    def set_pk(self, context: OpExecutionContext, table_name: str, pk_column: str, datatype: str):
        alter_column_query = f"""
        ALTER TABLE {table_name}
        ALTER COLUMN {pk_column} {datatype} NOT NULL;
        """
        add_pk_query = f"""
        ALTER TABLE {table_name}
        ADD CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_column});
        """
        try:
            context.log.info(f"Ensuring column {pk_column} in {table_name} is NOT NULL before setting primary key")
            with self.get_engine().connect() as conn:
                conn.execute(text(alter_column_query))
                conn.execute(text(add_pk_query))
                conn.commit()
            context.log.info(f"Successfully set primary key for {table_name}")
        except Exception as e:
            context.log.error(f"Failed to set primary key for {table_name}: {e}")
            raise Exception(f"Failed to set primary key for {table_name}: {e}")

    def set_fk(self, context: OpExecutionContext, table_name: str, fk_column: str, ref_table: str, ref_column: str):
        query = f"""
        ALTER TABLE {table_name}
        ADD CONSTRAINT FK_{fk_column} FOREIGN KEY ({fk_column}) REFERENCES {ref_table}({ref_column});
        """
        try:
            context.log.info(f"Setting foreign key for {table_name} on column {fk_column} referencing {ref_table}({ref_column})")
            with self.get_engine().connect() as conn:
                conn.execute(text(query))
                conn.commit()
            context.log.info(f"Successfully set foreign key for {table_name}")
        except Exception as e:
            context.log.error(f"Failed to set foreign key for {table_name}: {e}")
            raise Exception(f"Failed to set foreign key for {table_name}: {e}")

    def drop_fk(self, context: OpExecutionContext, table_name: str, fk_column: str):
        query = f"""
        ALTER TABLE {table_name}
        DROP CONSTRAINT FK_{fk_column};
        """
        try:
            context.log.info(f"Dropping foreign key {fk_column} from {table_name}")
            with self.get_engine().connect() as conn:
                conn.execute(text(query))
                conn.commit()
            context.log.info(f"Successfully dropped foreign key {fk_column} from {table_name}")
        except Exception as e:
            context.log.error(f"Failed to drop foreign key {fk_column} from {table_name}: {e}")
            raise Exception(f"Failed to drop foreign key {fk_column} from {table_name}: {e}")
    
    def get_table_schema(self, context: OpExecutionContext, table_name: str) -> pd.DataFrame:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :table_name
        """
        try:
            return self.execute_query(context, query, {"table_name": table_name})
        except Exception as e:
            context.log.error(f"Failed to get schema for {table_name}: {e}")
            raise
    
    def set_type(self, context: OpExecutionContext, table_name: str, column_name: str, new_type: str):
        query = f"""
        ALTER TABLE {table_name}
        ALTER COLUMN {column_name} {new_type};
        """
        try:
            context.log.info(f"Changing type of {column_name} in {table_name} to {new_type}")
            with self.get_engine().connect() as conn:
                conn.execute(text(query))
                conn.commit()
            context.log.info(f"Successfully changed type of {column_name} in {table_name} to {new_type}")
        except Exception as e:
            context.log.error(f"Failed to change type for {column_name} in {table_name}: {e}")
            raise Exception(f"Failed to change type for {column_name} in {table_name}: {e}")
        
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
class WorldBankConfig:
    base_url: str
    start_year: int
    end_year: int
    indicators: list[str]
    timeout: int
    max_page_size: int


@dataclass
class ETLConfigResource:
    data_directory: str
    log_level: str
