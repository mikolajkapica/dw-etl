"""
Data extraction operations for Himalayan Expeditions ETL pipeline.

This module contains Dagster ops for extracting data from various sources
including CSV files and the World Bank API.
"""

from typing import Dict, Any
import pandas as pd
from dagster import (
    op,
    Out,
    OpExecutionContext,
    RetryPolicy,
    Backoff,
    Jitter,
)

from ..resources import FileSystemResource, ETLConfigResource


@op(
    name="extract_expeditions_data",
    description="Extract expedition data from CSV file",
    out=Out(pd.DataFrame, description="Raw expeditions DataFrame"),
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=1.0,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    required_resource_keys={"fs", "etl_config"},
)
def extract_expeditions_data(context: OpExecutionContext) -> pd.DataFrame:
    """
    Extract expeditions data from expeditions.csv file.
    
    This op reads the expeditions CSV file and performs initial validation
    to ensure required columns are present and data types are reasonable.
      Returns:
        pd.DataFrame: Raw expeditions data with all original columns
    """
    filesystem: FileSystemResource = context.resources.fs

    config: ETLConfigResource = context.resources.etl_config
    
    context.log.info("Starting expeditions data extraction")
    
    try:     
        df = filesystem.read_csv(filename="expeditions.csv")
        
        context.log.info(f"Loaded {len(df)} expedition records")
        context.log.info(f"Columns: {list(df.columns)}")
        
        # Basic validation
        if df.empty:
            raise ValueError("Expeditions file is empty")
        
        # Check for required columns (adjust based on actual schema)
        required_columns = ["EXPID", "PEAKID", "YEAR", "SEASON"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Log data quality metrics
        total_rows = len(df)
        duplicate_expids = df["EXPID"].duplicated().sum()
        missing_peakids = df["PEAKID"].isnull().sum()
        
        context.log.info(f"Data quality metrics:")
        context.log.info(f"  Total rows: {total_rows}")
        context.log.info(f"  Duplicate EXPIDs: {duplicate_expids}")
        context.log.info(f"  Missing PEAKID: {missing_peakids}")
        
        if duplicate_expids > 0:
            context.log.warning(f"Found {duplicate_expids} duplicate expedition IDs")
        
        return df
        
    except Exception as e:
        context.log.error(f"Failed to extract expeditions data: {str(e)}")
        raise


@op(
    name="extract_members_data",
    description="Extract expedition members data from CSV file",
    out=Out(pd.DataFrame, description="Raw members DataFrame"),
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    required_resource_keys={"fs", "etl_config"},
)
def extract_members_data(context: OpExecutionContext) -> pd.DataFrame:
    """
    Extract expedition members data from members.csv file.
    
    This op reads the members CSV file and performs basic validation
    to ensure data quality and completeness.
      Returns:
        pd.DataFrame: Raw members data with all original columns
    """
    filesystem: FileSystemResource = context.resources.fs
    context.log.info("Starting members data extraction")
    
    try:
        df = filesystem.read_csv(filename="members.csv")
        
        context.log.info(f"Loaded {len(df)} member records")
        
        # Basic validation
        if df.empty:
            raise ValueError("Members file is empty")
        
        # Check for required columns
        required_columns = ["EXPID", "FNAME", "LNAME"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Log data quality metrics
        total_rows = len(df)
        missing_names = (df["FNAME"].isnull() & df["LNAME"].isnull()).sum()
        unique_expeditions = df["EXPID"].nunique()
        
        context.log.info(f"Data quality metrics:")
        context.log.info(f"  Total member records: {total_rows}")
        context.log.info(f"  Records missing both names: {missing_names}")
        context.log.info(f"  Unique expeditions: {unique_expeditions}")
        
        return df
        
    except Exception as e:
        context.log.error(f"Failed to extract members data: {str(e)}")
        raise


@op(
    name="extract_peaks_data", 
    description="Extract peaks data from CSV file",
    out=Out(pd.DataFrame, description="Raw peaks DataFrame"),
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    required_resource_keys={"fs", "etl_config"},
)
def extract_peaks_data(context: OpExecutionContext) -> pd.DataFrame:
    """
    Extract peaks data from peaks.csv file.
    
    This op reads the peaks CSV file containing mountain information
    and performs validation on peak identifiers and measurements.
      Returns:
        pd.DataFrame: Raw peaks data with all original columns
    """
    filesystem: FileSystemResource = context.resources.fs
    context.log.info("Starting peaks data extraction")
    
    try:
        df = filesystem.read_csv(filename="peaks.csv")
        
        context.log.info(f"Loaded {len(df)} peak records")
        
        # Basic validation
        if df.empty:
            raise ValueError("Peaks file is empty")
        
        # Check for required columns
        required_columns = ["PEAKID", "PKNAME", "HEIGHTM"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Log data quality metrics
        total_rows = len(df)
        duplicate_peak_ids = df["PEAKID"].duplicated().sum()
        missing_heights = df["HEIGHTM"].isnull().sum()
        unique_peaks = df["PEAKID"].nunique()
        
        context.log.info(f"Data quality metrics:")
        context.log.info(f"  Total peak records: {total_rows}")
        context.log.info(f"  Duplicate peak IDs: {duplicate_peak_ids}")
        context.log.info(f"  Missing heights: {missing_heights}")
        context.log.info(f"  Unique peaks: {unique_peaks}")
        
        return df
        
    except Exception as e:
        context.log.error(f"Failed to extract peaks data: {str(e)}")
        raise
