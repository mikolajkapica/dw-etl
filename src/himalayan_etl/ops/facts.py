"""
Fact table operations for the Himalayan Expeditions ETL process.
Handles FACT_Expeditions and BRIDGE_ExpeditionMembers table processing.
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Tuple
from dagster import op, In, Out, RetryPolicy, DagsterLogManager, Config

from himalayan_etl.resources import DatabaseResource, ETLConfigResource

logger = logging.getLogger(__name__)


class FactConfig(Config):
    """Configuration for fact table operations."""
    batch_size: int = 10000
    enable_validation: bool = True
    min_year: int = 1900
    max_year: int = 2050


@op(
    ins={
        "cleaned_expeditions": In(pd.DataFrame),
        "cleaned_members": In(pd.DataFrame),
        "dim_date": In(pd.DataFrame),
        "dim_nationality": In(pd.DataFrame),
        "dim_peak": In(pd.DataFrame),
        "dim_route": In(pd.DataFrame),
        "dim_expedition_status": In(pd.DataFrame),
        "dim_host_country": In(pd.DataFrame),
        "dim_member": In(pd.DataFrame)
    },
    out=Out(pd.DataFrame, description="Prepared fact table data"),
    config_schema=FactConfig,
    retry_policy=RetryPolicy(max_retries=3, delay=2),
    description="Transform and join data for the expeditions fact table"
)
def prepare_fact_expeditions(
    context,
    cleaned_expeditions: pd.DataFrame,
    cleaned_members: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_nationality: pd.DataFrame,
    dim_peak: pd.DataFrame,
    dim_route: pd.DataFrame,
    dim_expedition_status: pd.DataFrame,
    dim_host_country: pd.DataFrame,
    dim_member: pd.DataFrame,
    etl_config: ETLConfigResource,
    db: DatabaseResource
) -> pd.DataFrame:
    """
    Transform and join data to create the fact table.
    
    This op performs the core transformation to create the fact table by:
    1. Joining expeditions with all dimension tables
    2. Creating derived metrics and calculations
    3. Handling missing dimension keys
    4. Validating fact table integrity
    """
    
    context.log.info(f"Starting fact table preparation with {len(cleaned_expeditions)} expeditions")
    
    try:
        # Start with cleaned expeditions
        fact_df = cleaned_expeditions.copy()
        
        # Create date key for joining
        fact_df['DATE_KEY'] = fact_df['YEAR'].astype(str) + \
                             fact_df['SEASON_CODE'].astype(str).str.zfill(2) + '01'
        fact_df['DATE_KEY'] = pd.to_numeric(fact_df['DATE_KEY'], errors='coerce')
        
        # Join with date dimension
        fact_df = fact_df.merge(
            dim_date[['DATE_KEY', 'DATE_ID']],
            on='DATE_KEY',
            how='left'
        )
        
        # Join with peak dimension
        fact_df = fact_df.merge(
            dim_peak[['PEAK_ID', 'PEAK_NAME']],
            left_on='PEAK_ID',
            right_on='PEAK_NAME',
            how='left',
            suffixes=('', '_DIM')
        )
        fact_df['PEAK_KEY'] = fact_df['PEAK_ID_DIM']
        
        # Join with host country dimension
        fact_df = fact_df.merge(
            dim_host_country[['COUNTRY_KEY', 'COUNTRY_NAME']],
            left_on='HOST_COUNTRY',
            right_on='COUNTRY_NAME',
            how='left'
        )
        
        # Join with expedition status dimension
        fact_df = fact_df.merge(
            dim_expedition_status[['STATUS_KEY', 'TERMINATION_REASON_CODE']],
            left_on='TERMINATION_REASON_CODE',
            right_on='TERMINATION_REASON_CODE',
            how='left'
        )
        
        # Join with route dimension
        fact_df = fact_df.merge(
            dim_route[['ROUTE_KEY', 'PEAK_ID', 'ROUTE_NAME']],
            left_on=['PEAK_ID', 'ROUTE1'],
            right_on=['PEAK_ID', 'ROUTE_NAME'],
            how='left'
        )
        
        # Calculate expedition-level aggregates from members
        member_aggs = cleaned_members.groupby('EXPID').agg({
            'MEMBER_ID': 'count',
            'DEATH': 'sum',
            'SUCCESS': 'sum',
            'INJURY': 'sum',
            'AGE': ['mean', 'min', 'max']
        }).round(2)
        
        # Flatten column names
        member_aggs.columns = [
            'TOTAL_MEMBERS', 'TOTAL_DEATHS', 'TOTAL_SUCCESS', 
            'TOTAL_INJURIES', 'AVG_MEMBER_AGE', 'MIN_MEMBER_AGE', 'MAX_MEMBER_AGE'
        ]
        member_aggs = member_aggs.reset_index()
        
        # Join member aggregates
        fact_df = fact_df.merge(member_aggs, on='EXPID', how='left')
        
        # Fill missing aggregates with 0
        agg_columns = ['TOTAL_MEMBERS', 'TOTAL_DEATHS', 'TOTAL_SUCCESS', 
                      'TOTAL_INJURIES', 'AVG_MEMBER_AGE', 'MIN_MEMBER_AGE', 'MAX_MEMBER_AGE']
        for col in agg_columns:
            fact_df[col] = fact_df[col].fillna(0)
        
        # Calculate derived metrics
        fact_df['SUCCESS_RATE'] = (fact_df['TOTAL_SUCCESS'] / fact_df['TOTAL_MEMBERS'].replace(0, 1)).round(4)
        fact_df['DEATH_RATE'] = (fact_df['TOTAL_DEATHS'] / fact_df['TOTAL_MEMBERS'].replace(0, 1)).round(4)
        fact_df['INJURY_RATE'] = (fact_df['TOTAL_INJURIES'] / fact_df['TOTAL_MEMBERS'].replace(0, 1)).round(4)
        
        # Duration calculations
        fact_df['EXPEDITION_DURATION_DAYS'] = (
            pd.to_datetime(fact_df['TERM_DATE'], errors='coerce') - 
            pd.to_datetime(fact_df['BASE_DATE'], errors='coerce')
        ).dt.days
        
        # Handle missing dimension keys with default values
        default_keys = {
            'DATE_ID': -1,
            'PEAK_KEY': -1,
            'COUNTRY_KEY': -1,
            'STATUS_KEY': -1,
            'ROUTE_KEY': -1
        }
        
        for key, default_value in default_keys.items():
            if key in fact_df.columns:
                fact_df[key] = fact_df[key].fillna(default_value)
        
        # Select final fact table columns
        fact_columns = [
            'EXPID', 'DATE_ID', 'PEAK_KEY', 'COUNTRY_KEY', 'STATUS_KEY', 'ROUTE_KEY',
            'YEAR', 'SEASON_CODE', 'TOTAL_MEMBERS', 'TOTAL_DEATHS', 'TOTAL_SUCCESS',
            'TOTAL_INJURIES', 'AVG_MEMBER_AGE', 'MIN_MEMBER_AGE', 'MAX_MEMBER_AGE',
            'SUCCESS_RATE', 'DEATH_RATE', 'INJURY_RATE', 'EXPEDITION_DURATION_DAYS',
            'CLAIMED_HEIGHT', 'SMTDATE_ACCURACY', 'BASECAMP_HEIGHT'
        ]
        
        # Keep only columns that exist
        available_columns = [col for col in fact_columns if col in fact_df.columns]
        fact_df = fact_df[available_columns]
          # Data validation if enabled
        if context.op_config["enable_validation"]:
            fact_df = _validate_fact_data(fact_df, context.op_config, context.log)
        
        context.log.info(f"Prepared fact table with {len(fact_df)} records")
        
        return fact_df
        
    except Exception as e:
        context.log.error(f"Error preparing fact table: {str(e)}")
        raise


@op(
    ins={
        "cleaned_expeditions": In(pd.DataFrame),
        "cleaned_members": In(pd.DataFrame),
        "dim_member": In(pd.DataFrame)
    },
    out=Out(pd.DataFrame, description="Bridge table data"),
    config_schema=FactConfig,
    retry_policy=RetryPolicy(max_retries=3, delay=2),
    description="Create bridge table for expedition-member relationships"
)
def prepare_bridge_expedition_members(
    context,
    cleaned_expeditions: pd.DataFrame,
    cleaned_members: pd.DataFrame,
    dim_member: pd.DataFrame
) -> pd.DataFrame:
    """
    Create the bridge table for many-to-many expedition-member relationships.
    """
    
    context.log.info(f"Creating bridge table for {len(cleaned_members)} member records")
    
    try:
        # Start with cleaned members
        bridge_df = cleaned_members.copy()
        
        # Join with member dimension to get member keys
        bridge_df = bridge_df.merge(
            dim_member[['MEMBER_KEY', 'MEMBER_ID']],
            on='MEMBER_ID',
            how='left'
        )
        
        # Select bridge table columns
        bridge_columns = [
            'EXPID', 'MEMBER_KEY', 'MEMBER_ID', 'SUCCESS', 'DEATH', 'INJURY',
            'HIRED', 'LEADER', 'OXYGEN_USED', 'CLIMBING_STATUS'
        ]
        
        # Keep only columns that exist
        available_columns = [col for col in bridge_columns if col in bridge_df.columns]
        bridge_df = bridge_df[available_columns]
        
        # Handle missing member keys
        bridge_df['MEMBER_KEY'] = bridge_df['MEMBER_KEY'].fillna(-1)
        
        # Remove duplicates
        bridge_df = bridge_df.drop_duplicates(subset=['EXPID', 'MEMBER_KEY'])
        
        context.log.info(f"Created bridge table with {len(bridge_df)} relationships")
        
        return bridge_df
        
    except Exception as e:
        context.log.error(f"Error creating bridge table: {str(e)}")
        raise


@op(
    ins={"fact_data": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Load results"),
    config_schema=FactConfig,
    retry_policy=RetryPolicy(max_retries=3, delay=2),
    description="Load fact table data into the database"
)
def load_fact_expeditions(
    context,
    fact_data: pd.DataFrame,
    db: DatabaseResource
) -> Dict[str, Any]:
    """
    Load the prepared fact table data into the database.
    """
    
    context.log.info(f"Loading {len(fact_data)} fact records to database")
    
    try:
        # Load data using bulk insert
        load_result = db.bulk_insert_dataframe(
            dataframe=fact_data,
            table_name='FACT_Expeditions',
            batch_size=context.op_config["batch_size"]
        )
        
        context.log.info(f"Successfully loaded fact table: {load_result}")
        
        return {
            'table_name': 'FACT_Expeditions',
            'records_loaded': len(fact_data),
            'load_timestamp': pd.Timestamp.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        context.log.error(f"Error loading fact table: {str(e)}")
        raise


@op(
    ins={"bridge_data": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Load results"),
    config_schema=FactConfig,
    retry_policy=RetryPolicy(max_retries=3, delay=2),
    description="Load bridge table data into the database"
)
def load_bridge_expedition_members(
    context,
    bridge_data: pd.DataFrame,
    db: DatabaseResource
) -> Dict[str, Any]:
    """
    Load the bridge table data into the database.
    """
    
    context.log.info(f"Loading {len(bridge_data)} bridge records to database")
    
    try:
        # Load data using bulk insert
        load_result = db.bulk_insert_dataframe(
            dataframe=bridge_data,
            table_name='BRIDGE_ExpeditionMembers',
            batch_size=context.op_config["batch_size"]
        )
        
        context.log.info(f"Successfully loaded bridge table: {load_result}")
        
        return {
            'table_name': 'BRIDGE_ExpeditionMembers',
            'records_loaded': len(bridge_data),
            'load_timestamp': pd.Timestamp.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        context.log.error(f"Error loading bridge table: {str(e)}")
        raise


def _validate_fact_data(
    fact_df: pd.DataFrame, 
    op_config: Dict[str, Any], 
    logger: DagsterLogManager
) -> pd.DataFrame:
    """
    Validate fact table data quality and consistency.
    """
    
    initial_count = len(fact_df)
    
    # Remove records with invalid years
    if 'YEAR' in fact_df.columns:
        valid_year_mask = (
            (fact_df['YEAR'] >= op_config["min_year"]) &
            (fact_df['YEAR'] <= op_config["max_year"])
        )
        fact_df = fact_df[valid_year_mask]
        
        removed_count = initial_count - len(fact_df)
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} records with invalid years")
    
    # Validate rates are between 0 and 1
    rate_columns = ['SUCCESS_RATE', 'DEATH_RATE', 'INJURY_RATE']
    for col in rate_columns:
        if col in fact_df.columns:
            fact_df[col] = fact_df[col].clip(0, 1)
    
    # Validate member counts are non-negative
    count_columns = ['TOTAL_MEMBERS', 'TOTAL_DEATHS', 'TOTAL_SUCCESS', 'TOTAL_INJURIES']
    for col in count_columns:
        if col in fact_df.columns:
            fact_df[col] = fact_df[col].clip(lower=0)
    
    logger.info(f"Data validation completed. Final record count: {len(fact_df)}")
    
    return fact_df
