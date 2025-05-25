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
    Extract expeditions data from exped.csv file.
    
    This op reads the expeditions CSV file and performs initial validation
    to ensure required columns are present and data types are reasonable.
      Returns:
        pd.DataFrame: Raw expeditions data with all original columns
    """
    filesystem: FileSystemResource = context.resources.fs
    config: ETLConfigResource = context.resources.etl_config
    
    context.log.info("Starting expeditions data extraction")
    
    try:
        # Read expeditions CSV file
        df = filesystem.read_csv("exped.csv")
        
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
        df = filesystem.read_csv("members.csv")
        
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
        df = filesystem.read_csv("peaks.csv")
        
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


@op(
    name="extract_references_data",
    description="Extract literature references data from CSV file", 
    out=Out(pd.DataFrame, description="Raw references DataFrame"),
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    required_resource_keys={"fs", "etl_config"},
)
def extract_references_data(context: OpExecutionContext) -> pd.DataFrame:
    """
    Extract literature references data from refer.csv file.
    
    This op reads the references CSV file containing bibliography
    information related to expeditions.
      Returns:
        pd.DataFrame: Raw references data with all original columns
    """
    filesystem: FileSystemResource = context.resources.fs
    
    context.log.info("Starting references data extraction")
    
    try:
        df = filesystem.read_csv("refer.csv")
        
        context.log.info(f"Loaded {len(df)} reference records")
        
        # Basic validation - references might have different structure
        if df.empty:
            context.log.warning("References file is empty - this might be expected")
            return pd.DataFrame()  # Return empty DataFrame
        
        # Log basic statistics
        total_rows = len(df)
        context.log.info(f"Total reference records: {total_rows}")
        context.log.info(f"Columns: {list(df.columns)}")
        
        return df
        
    except FileNotFoundError:
        context.log.warning("References file not found - continuing without references")
        return pd.DataFrame()
    except Exception as e:
        context.log.error(f"Failed to extract references data: {str(e)}")
        raise


@op(
    name="extract_world_bank_data",
    description="Extract World Bank development indicators data",
    out=Out(pd.DataFrame, description="Raw World Bank indicators DataFrame"),
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    required_resource_keys={"fs", "etl_config"},
)
def extract_world_bank_data(context: OpExecutionContext) -> pd.DataFrame:
    """
    Extract World Bank development indicators from WDI CSV file.
    
    This op reads the World Development Indicators CSV file containing
    economic and social indicators by country and year.
      Returns:
        pd.DataFrame: Raw WDI data with country, indicator, and year columns
    """
    filesystem: FileSystemResource = context.resources.fs
    
    context.log.info("Starting World Bank data extraction")
    
    try:
        df = filesystem.read_csv("wdi.csv")
        
        context.log.info(f"Loaded World Bank data with shape: {df.shape}")
        
        # Basic validation
        if df.empty:
            raise ValueError("World Bank indicators file is empty")
        
        # Check for expected WDI structure (pivot format with years as columns)
        expected_columns = ["Country Name", "Country Code", "Series Name", "Series Code"]
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing expected WDI columns: {missing_columns}")
        
        # Identify year columns (columns that look like years)
        year_columns = [col for col in df.columns 
                       if col.replace(" [YR", "").replace("]", "").isdigit() 
                       and 1960 <= int(col.replace(" [YR", "").replace("]", "")) <= 2030]
        
        context.log.info(f"Found {len(year_columns)} year columns")
        
        # Log data quality metrics
        total_rows = len(df)
        unique_countries = df["Country Code"].nunique()
        unique_indicators = df["Series Code"].nunique()
        
        context.log.info(f"Data quality metrics:")
        context.log.info(f"  Total indicator-country combinations: {total_rows}")
        context.log.info(f"  Unique countries: {unique_countries}")
        context.log.info(f"  Unique indicators: {unique_indicators}")
        context.log.info(f"  Year range: {min(year_columns)} to {max(year_columns)}")
        
        return df
        
    except FileNotFoundError:
        context.log.warning("World Bank data file not found - continuing without WDI data")
        return pd.DataFrame()
    except Exception as e:
        context.log.error(f"Failed to extract World Bank data: {str(e)}")
        raise


@op(
    name="validate_extracted_data",
    description="Validate completeness and quality of extracted data",
    out=Out(Dict[str, Any], description="Data validation results"),
    required_resource_keys={"etl_config"},
)
def validate_extracted_data(
    context: OpExecutionContext,
    expeditions_df: pd.DataFrame,
    members_df: pd.DataFrame,
    peaks_df: pd.DataFrame,
    references_df: pd.DataFrame,
    world_bank_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Validate the completeness and quality of all extracted data sources.
    
    This op performs cross-dataset validation to ensure referential integrity
    and data consistency across all source files.
    
    Args:
        expeditions_df: Expeditions data
        members_df: Members data
        peaks_df: Peaks data
        references_df: References data
        world_bank_df: World Bank data
        
    Returns:
        Dict containing validation results and quality metrics
    """
    config: ETLConfigResource = context.resources.etl_config
    
    context.log.info("Starting data validation")
    
    validation_results = {
        "total_expeditions": len(expeditions_df),
        "total_members": len(members_df),
        "total_peaks": len(peaks_df),
        "total_references": len(references_df),
        "total_wb_records": len(world_bank_df),
        "quality_issues": [],
        "data_quality_score": 1.0,
    }
    
    try:
        # Cross-dataset referential integrity checks
        if not expeditions_df.empty and not peaks_df.empty:
            # Check if all expedition peaks exist in peaks table
            exp_peaks = set(expeditions_df["PEAKID"].dropna())
            available_peaks = set(peaks_df["PEAKID"].dropna())
            missing_peaks = exp_peaks - available_peaks
            
            if missing_peaks:
                issue = f"Expeditions reference {len(missing_peaks)} peaks not in peaks table"
                validation_results["quality_issues"].append(issue)
                context.log.warning(issue)
        
        if not expeditions_df.empty and not members_df.empty:
            # Check if all member expeditions exist in expeditions table
            member_expeditions = set(members_df["EXPID"].dropna())
            available_expeditions = set(expeditions_df["EXPID"].dropna())
            orphaned_members = member_expeditions - available_expeditions
            
            if orphaned_members:
                issue = f"Members reference {len(orphaned_members)} expeditions not in expeditions table"
                validation_results["quality_issues"].append(issue)
                context.log.warning(issue)
        
        # Calculate overall data quality score
        total_issues = len(validation_results["quality_issues"])
        if total_issues == 0:
            validation_results["data_quality_score"] = 1.0
        else:
            # Simple scoring: reduce score by 0.1 for each major issue
            validation_results["data_quality_score"] = max(0.0, 1.0 - (total_issues * 0.1))
        
        context.log.info(f"Data validation completed")
        context.log.info(f"Quality score: {validation_results['data_quality_score']:.2f}")
        context.log.info(f"Issues found: {total_issues}")
        
        # Check if quality meets threshold
        if validation_results["data_quality_score"] < config.data_quality_threshold:
            context.log.warning(
                f"Data quality score {validation_results['data_quality_score']:.2f} "
                f"is below threshold {config.data_quality_threshold}"
            )
        
        return validation_results
        
    except Exception as e:
        context.log.error(f"Data validation failed: {str(e)}")
        validation_results["quality_issues"].append(f"Validation error: {str(e)}")
        validation_results["data_quality_score"] = 0.0
        return validation_results
