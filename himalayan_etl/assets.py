"""
Dagster asset definitions for the Himalayan Expeditions data warehouse.
Uses Dagster's asset-based approach for better lineage and dependency management.
"""

import pandas as pd
from typing import Dict, Any, Optional
from dagster import (
    asset, 
    AssetIn, 
    Field, Bool, Int,
    RetryPolicy,
    MetadataValue,
    AssetMaterialization,
    Output,
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    StaticPartitionsDefinition
)
from datetime import datetime, timedelta

from himalayan_etl.resources import DatabaseResource, FileSystemResource, ETLConfigResource

# Import operation functions for reusing logic
# from himalayan_etl.ops.extraction import (
#     extract_expeditions_data,
#     extract_members_data,
#     extract_peaks_data,
#     extract_world_bank_data
# )

from himalayan_etl.ops.cleaning import (
    clean_expeditions_data as _clean_expeditions_data,
    clean_members_data as _clean_members_data,
    clean_peaks_data as _clean_peaks_data
)

from himalayan_etl.ops.dimensions import (
    # create_date_dimension as _create_date_dimension,
    create_dim_nationality as _create_dim_nationality,
    create_dim_peak as _create_dim_peak,
    create_dim_member as _create_dim_member
)

from himalayan_etl.ops.facts import (
    prepare_fact_expeditions as _prepare_fact_expeditions
)

from himalayan_etl.ops.world_bank import (
    extract_world_bank_data as _extract_world_bank_data,
    clean_world_bank_data as _clean_world_bank_data
)


# Configuration schema for assets
asset_config_schema = {
    "enable_validation": Field(Bool, default_value=True),
    "batch_size": Field(Int, default_value=10000)
}


# Partitions for time-based assets
yearly_partitions = StaticPartitionsDefinition([str(year) for year in range(1900, 2025)])
monthly_partitions = MonthlyPartitionsDefinition(start_date="2020-01-01")


# Raw Data Assets
@asset(
    name="raw_expeditions",
    description="Raw expeditions data from CSV file",
    compute_kind="pandas",
    retry_policy=RetryPolicy(max_retries=3, delay=2)
)
def raw_expeditions(
    context,
    fs: FileSystemResource,
    etl_config: ETLConfigResource
) -> pd.DataFrame:
    """Load raw expeditions data from CSV file."""
    
    context.log.info("Loading raw expeditions data")
    
    # Use the existing extraction logic
    df = fs.read_csv_file("exped.csv")
    
    context.add_output_metadata({
        "num_records": len(df),
        "num_columns": len(df.columns),
        "file_size_bytes": MetadataValue.int(df.memory_usage(deep=True).sum()),
        "date_range": f"{df['YEAR'].min()} - {df['YEAR'].max()}" if 'YEAR' in df.columns else "Unknown"
    })
    
    return df


@asset(
    name="raw_members", 
    description="Raw members data from CSV file",
    compute_kind="pandas",
    retry_policy=RetryPolicy(max_retries=3, delay=2)
)
def raw_members(
    context,
    fs: FileSystemResource,
    etl_config: ETLConfigResource
) -> pd.DataFrame:
    """Load raw members data from CSV file."""
    
    context.log.info("Loading raw members data")
    
    df = fs.read_csv_file("members.csv")
    
    context.add_output_metadata({
        "num_records": len(df),
        "num_columns": len(df.columns),
        "file_size_bytes": MetadataValue.int(df.memory_usage(deep=True).sum()),
        "unique_expeditions": df['EXPID'].nunique() if 'EXPID' in df.columns else 0
    })
    
    return df


@asset(
    name="raw_peaks",
    description="Raw peaks data from CSV file", 
    compute_kind="pandas",
    retry_policy=RetryPolicy(max_retries=3, delay=2)
)
def raw_peaks(
    context,
    fs: FileSystemResource,
    etl_config: ETLConfigResource
) -> pd.DataFrame:
    """Load raw peaks data from CSV file."""
    
    context.log.info("Loading raw peaks data")
    
    df = fs.read_csv_file("peaks.csv")
    
    context.add_output_metadata({
        "num_records": len(df),
        "num_columns": len(df.columns),
        "unique_peaks": df['PEAK_ID'].nunique() if 'PEAK_ID' in df.columns else 0,
        "height_range": f"{df['HEIGHT_M'].min()} - {df['HEIGHT_M'].max()}" if 'HEIGHT_M' in df.columns else "Unknown"
    })
    
    return df


@asset(
    name="raw_world_bank_data",
    description="Raw World Bank indicators data from API",
    compute_kind="requests",
    partitions_def=monthly_partitions,
    retry_policy=RetryPolicy(max_retries=3, delay=5)
)
def raw_world_bank_data(
    context,
    etl_config: ETLConfigResource
) -> pd.DataFrame:
    """Extract World Bank data from API."""
    
    context.log.info("Extracting World Bank data from API")
    
    # Mock implementation - replace with actual API call
    # In practice, you'd use the _extract_world_bank_data function
    df = pd.DataFrame({
        'country_code': ['NPL', 'CHN', 'IND'],
        'country_name': ['Nepal', 'China', 'India'], 
        'indicator_code': ['NY.GDP.PCAP.CD'] * 3,
        'year': [2022] * 3,
        'value': [1208.0, 12720.0, 2388.0]
    })
    
    context.add_output_metadata({
        "num_records": len(df),
        "countries_covered": df['country_code'].nunique(),
        "indicators_covered": df['indicator_code'].nunique(),
        "extraction_timestamp": MetadataValue.text(datetime.now().isoformat())
    })
    
    return df


# Cleaned Data Assets
@asset(
    name="cleaned_expeditions",
    ins={"raw_expeditions": AssetIn("raw_expeditions")},
    description="Cleaned and standardized expeditions data",
    compute_kind="pandas",
    config_schema=asset_config_schema
)
def cleaned_expeditions(
    context,
    raw_expeditions: pd.DataFrame,
    etl_config: ETLConfigResource
) -> pd.DataFrame:
    """Clean and standardize expeditions data."""
    
    context.log.info(f"Cleaning {len(raw_expeditions)} expedition records")
    
    # Apply cleaning logic (simplified - use actual cleaning functions)
    df = raw_expeditions.copy()
    
    # Basic cleaning
    initial_count = len(df)
    df = df.dropna(subset=['EXPID'])
    df = df.drop_duplicates(subset=['EXPID'])
    
    # Data quality metrics
    null_counts = df.isnull().sum()
    quality_score = 1.0 - (null_counts.sum() / (len(df) * len(df.columns)))
    
    context.add_output_metadata({
        "records_before_cleaning": initial_count,
        "records_after_cleaning": len(df),
        "records_removed": initial_count - len(df),
        "data_quality_score": MetadataValue.float(round(quality_score, 3)),
        "null_value_counts": MetadataValue.json(null_counts.to_dict())
    })
    
    return df


@asset(
    name="cleaned_members",
    ins={"raw_members": AssetIn("raw_members")},
    description="Cleaned and standardized members data",
    compute_kind="pandas",
    config_schema=asset_config_schema
)
def cleaned_members(
    context,
    raw_members: pd.DataFrame,
    etl_config: ETLConfigResource
) -> pd.DataFrame:
    """Clean and standardize members data."""
    
    context.log.info(f"Cleaning {len(raw_members)} member records")
    
    df = raw_members.copy()
    
    # Basic cleaning
    initial_count = len(df)
    df = df.dropna(subset=['EXPID', 'MEMBER_ID'])
    df = df.drop_duplicates(subset=['EXPID', 'MEMBER_ID'])
    
    context.add_output_metadata({
        "records_before_cleaning": initial_count,
        "records_after_cleaning": len(df),
        "unique_members": df['MEMBER_ID'].nunique(),
        "unique_expeditions": df['EXPID'].nunique()
    })
    
    return df


# Dimension Assets
@asset(
    name="dim_date",
    ins={"cleaned_expeditions": AssetIn("cleaned_expeditions")},
    description="Date dimension table",
    compute_kind="pandas"
)
def dim_date(
    context,
    cleaned_expeditions: pd.DataFrame,
    db: DatabaseResource
) -> pd.DataFrame:
    """Create date dimension table."""
    
    context.log.info("Creating date dimension")
    
    # Extract unique years and seasons
    years = cleaned_expeditions['YEAR'].dropna().unique() if 'YEAR' in cleaned_expeditions.columns else []
    seasons = [1, 2, 3, 4]  # Spring, Summer, Autumn, Winter
    
    date_records = []
    for year in sorted(years):
        for season in seasons:
            date_key = int(f"{year}{season:02d}01")
            date_records.append({
                'DATE_KEY': date_key,
                'DATE_ID': len(date_records) + 1,
                'YEAR': int(year),
                'SEASON_CODE': season,
                'SEASON_NAME': {1: 'Spring', 2: 'Summer', 3: 'Autumn', 4: 'Winter'}[season],
                'DECADE': f"{(int(year) // 10) * 10}s",
                'CENTURY': f"{(int(year) // 100) + 1}th Century"
            })
    
    dim_df = pd.DataFrame(date_records)
    
    # Load to database
    try:
        db.upsert_dataframe(
            dataframe=dim_df,
            table_name='DIM_Date',
            key_columns=['DATE_KEY']
        )
        context.log.info("Successfully loaded date dimension to database")
    except Exception as e:
        context.log.warning(f"Failed to load to database: {str(e)}")
    
    context.add_output_metadata({
        "num_date_records": len(dim_df),
        "year_range": f"{dim_df['YEAR'].min()} - {dim_df['YEAR'].max()}",
        "seasons_covered": len(dim_df['SEASON_CODE'].unique())
    })
    
    return dim_df


@asset(
    name="dim_peak",
    ins={"raw_peaks": AssetIn("raw_peaks")},
    description="Peak dimension table",
    compute_kind="pandas"
)
def dim_peak(
    context,
    raw_peaks: pd.DataFrame,
    db: DatabaseResource
) -> pd.DataFrame:
    """Create peak dimension table."""
    
    context.log.info("Creating peak dimension")
    
    df = raw_peaks.copy()
    
    # Basic processing
    df['PEAK_KEY'] = range(1, len(df) + 1)
    df = df.rename(columns={'PEAK_ID': 'PEAK_NAME'})
    
    # Load to database
    try:
        db.upsert_dataframe(
            dataframe=df,
            table_name='DIM_Peak',
            key_columns=['PEAK_NAME']
        )
        context.log.info("Successfully loaded peak dimension to database")
    except Exception as e:
        context.log.warning(f"Failed to load to database: {str(e)}")
    
    context.add_output_metadata({
        "num_peaks": len(df),
        "height_range": f"{df['HEIGHT_M'].min()} - {df['HEIGHT_M'].max()}" if 'HEIGHT_M' in df.columns else "Unknown"
    })
    
    return df


@asset(
    name="dim_member",
    ins={"cleaned_members": AssetIn("cleaned_members")},
    description="Member dimension table with SCD Type 2",
    compute_kind="pandas"
)
def dim_member(
    context,
    cleaned_members: pd.DataFrame,
    db: DatabaseResource
) -> pd.DataFrame:
    """Create member dimension table with Slowly Changing Dimension Type 2."""
    
    context.log.info("Creating member dimension with SCD Type 2")
    
    # Group by member to get latest information
    member_latest = cleaned_members.groupby('MEMBER_ID').agg({
        'MEMBER_NAME': 'first',
        'CITIZENSHIP': 'first', 
        'AGE': 'first',
        'GENDER': 'first',
        'EXPID': 'count'  # Number of expeditions
    }).reset_index()
    
    member_latest = member_latest.rename(columns={'EXPID': 'TOTAL_EXPEDITIONS'})
    member_latest['MEMBER_KEY'] = range(1, len(member_latest) + 1)
    member_latest['EFFECTIVE_DATE'] = datetime.now()
    member_latest['EXPIRY_DATE'] = pd.to_datetime('2099-12-31')
    member_latest['IS_CURRENT'] = True
    
    # Load to database
    try:
        db.upsert_dataframe(
            dataframe=member_latest,
            table_name='DIM_Member',
            key_columns=['MEMBER_ID']
        )
        context.log.info("Successfully loaded member dimension to database")
    except Exception as e:
        context.log.warning(f"Failed to load to database: {str(e)}")
    
    context.add_output_metadata({
        "unique_members": len(member_latest),
        "avg_expeditions_per_member": member_latest['TOTAL_EXPEDITIONS'].mean(),
        "gender_distribution": MetadataValue.json(
            member_latest['GENDER'].value_counts().to_dict() if 'GENDER' in member_latest.columns else {}
        )
    })
    
    return member_latest


# Fact Table Asset
@asset(
    name="fact_expeditions",
    ins={
        "cleaned_expeditions": AssetIn("cleaned_expeditions"),
        "cleaned_members": AssetIn("cleaned_members"),
        "dim_date": AssetIn("dim_date"),
        "dim_peak": AssetIn("dim_peak"),
        "dim_member": AssetIn("dim_member")
    },
        description="Main fact table for expeditions",
    compute_kind="pandas",
    config_schema=asset_config_schema
)
def fact_expeditions(
    context,
    cleaned_expeditions: pd.DataFrame,
    cleaned_members: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_peak: pd.DataFrame,
    dim_member: pd.DataFrame,
    db: DatabaseResource
) -> pd.DataFrame:
    """Create the main expeditions fact table."""
    
    context.log.info("Creating expeditions fact table")
    
    # Start with cleaned expeditions
    fact_df = cleaned_expeditions.copy()
    
    # Join with date dimension
    fact_df['DATE_KEY'] = fact_df['YEAR'].astype(str) + '0101'
    fact_df = fact_df.merge(
        dim_date[['DATE_KEY', 'DATE_ID']], 
        on='DATE_KEY', 
        how='left'
    )
    
    # Calculate member aggregates
    member_aggs = cleaned_members.groupby('EXPID').agg({
        'MEMBER_ID': 'count',
        'SUCCESS': 'sum',
        'DEATH': 'sum'
    }).reset_index()
    
    member_aggs.columns = ['EXPID', 'TOTAL_MEMBERS', 'TOTAL_SUCCESS', 'TOTAL_DEATHS']
    
    # Join member aggregates
    fact_df = fact_df.merge(member_aggs, on='EXPID', how='left')
    
    # Fill missing values
    fact_df[['TOTAL_MEMBERS', 'TOTAL_SUCCESS', 'TOTAL_DEATHS']] = fact_df[
        ['TOTAL_MEMBERS', 'TOTAL_SUCCESS', 'TOTAL_DEATHS']
    ].fillna(0)
    
    # Calculate success rate
    fact_df['SUCCESS_RATE'] = (
        fact_df['TOTAL_SUCCESS'] / fact_df['TOTAL_MEMBERS'].replace(0, 1)
    ).round(4)
    
    # Select final columns
    fact_columns = [
        'EXPID', 'DATE_ID', 'YEAR', 'TOTAL_MEMBERS', 
        'TOTAL_SUCCESS', 'TOTAL_DEATHS', 'SUCCESS_RATE'
    ]
    fact_df = fact_df[[col for col in fact_columns if col in fact_df.columns]]
    
    # Load to database
    try:
        db.bulk_insert_dataframe(
                    dataframe=fact_df,
            table_name='FACT_Expeditions',
            batch_size=context.op_config["batch_size"]
        )
        context.log.info("Successfully loaded fact table to database")
    except Exception as e:
        context.log.warning(f"Failed to load to database: {str(e)}")
    
    context.add_output_metadata({
        "num_expeditions": len(fact_df),
        "total_members": fact_df['TOTAL_MEMBERS'].sum(),
        "overall_success_rate": fact_df['SUCCESS_RATE'].mean(),
        "year_range": f"{fact_df['YEAR'].min()} - {fact_df['YEAR'].max()}"
    })
    
    return fact_df


# Data Quality Asset
@asset(
    name="data_quality_report",
    ins={"fact_expeditions": AssetIn("fact_expeditions")},
    description="Comprehensive data quality assessment",
    compute_kind="pandas"
)
def data_quality_report(
    context,
    fact_expeditions: pd.DataFrame,
    db: DatabaseResource
) -> Dict[str, Any]:
    """Generate comprehensive data quality report."""
    
    context.log.info("Generating data quality report")
    
    report = {
        'report_timestamp': datetime.now().isoformat(),
        'total_expeditions': len(fact_expeditions),
        'data_completeness': {},
        'data_quality_score': 0.0
    }
    
    # Calculate completeness metrics
    for column in fact_expeditions.columns:
        null_count = fact_expeditions[column].isnull().sum()
        completeness = 1.0 - (null_count / len(fact_expeditions))
        report['data_completeness'][column] = round(completeness, 3)
    
    # Overall quality score
    avg_completeness = sum(report['data_completeness'].values()) / len(report['data_completeness'])
    report['data_quality_score'] = round(avg_completeness, 3)
    
    context.add_output_metadata({
        "overall_quality_score": MetadataValue.float(report['data_quality_score']),
        "total_expeditions": MetadataValue.int(report['total_expeditions']),
        "report_timestamp": MetadataValue.text(report['report_timestamp'])
    })
    
    return report


# All assets are automatically discovered by Dagster
# No need for AssetGroup in newer versions
