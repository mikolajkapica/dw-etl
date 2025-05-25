"""
World Bank data integration operations for country indicators dimension.
Handles processing of World Development Indicators data.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from dagster import op, In, Out, RetryPolicy, Field, String, Int, List as DagsterList, Array as DagsterArray
import requests
from datetime import datetime

from himalayan_etl.resources import DatabaseResource, ETLConfigResource

logger = logging.getLogger(__name__)


# Configuration schema for World Bank data operations
world_bank_config_schema = {
    "base_url": Field(String, default_value="https://api.worldbank.org/v2"),
    "batch_size": Field(Int, default_value=1000),
    "timeout": Field(Int, default_value=30),
    "retry_attempts": Field(Int, default_value=3),
    "indicators": Field(DagsterArray(String), default_value=[
        "NY.GDP.PCAP.CD",      # GDP per capita (current US$) - GdpPerCapita
        "HD.HCI.OVRL",         # Human Capital Index (HCI) overall - HumanCapitalIndex
        "IT.NET.USER.ZS",      # Individuals using the Internet (% of population) - InternetUsersPercent
        "SH.MED.PHYS.ZS",      # Physicians (per 1,000 people) - PhysiciansPer1000
        "PV.EST",              # Political Stability and Absence of Violence/Terrorism: Estimate - PoliticalStabilityIndex
    ]),
    "start_year": Field(Int, default_value=1960),
    "end_year": Field(Int, default_value=2023)
}


@op(
    out=Out(pd.DataFrame, description="Raw World Bank data"),
    config_schema=world_bank_config_schema,
    retry_policy=RetryPolicy(max_retries=3, delay=5),
    description="Extract World Development Indicators data from World Bank API",
    required_resource_keys={"fs", "etl_config"},
)
def extract_world_bank_data(
    context,
) -> pd.DataFrame:
    """
    Extract World Development Indicators data from the World Bank API.
    
    This op fetches economic and social indicators for all countries
    that appear in the Himalayan expeditions data.
    """
    context.log.info(f"Starting World Bank data extraction for {len(context.op_config['indicators'])} indicators")

    etl_config: ETLConfigResource = context.resources.etl_config
    
    try:
        all_data = []
        
        for indicator in context.op_config["indicators"]:
            context.log.info(f"Fetching data for indicator: {indicator}")
            
            # Construct API URL
            url = f"{context.op_config['base_url']}/country/all/indicator/{indicator}" 
            params = {
                'format': 'json',
                'date': f"{context.op_config['start_year']}:{context.op_config['end_year']}",
                'per_page': 10000,
                'page': 1
            }
            try:
                response = requests.get(url, params=params, timeout=context.op_config['timeout'])
                response.raise_for_status()
                
                data = response.json()
                
                if len(data) >= 2 and data[1]:
                    # Parse the data
                    for record in data[1]:
                        if record['value'] is not None:
                            all_data.append({
                                'country_code': record['country']['id'],
                                'country_name': record['country']['value'],
                                'indicator_code': record['indicator']['id'],
                                'indicator_name': record['indicator']['value'],
                                'year': int(record['date']),
                                'value': float(record['value']) if record['value'] else None
                            })
                
                context.log.info(f"Successfully fetched {len([d for d in all_data if d['indicator_code'] == indicator])} records for {indicator}")
                
            except requests.RequestException as e:
                context.log.warning(f"Failed to fetch data for indicator {indicator}: {str(e)}")
                continue
            
            except Exception as e:
                context.log.warning(f"Error processing data for indicator {indicator}: {str(e)}")
                continue
        
        # Convert to DataFrame
        if all_data:
            wb_df = pd.DataFrame(all_data)
            context.log.info(f"Extracted {len(wb_df)} total World Bank records")
            return wb_df
        else:
            context.log.warning("No World Bank data was successfully extracted")
            return pd.DataFrame()
            
    except Exception as e:
        context.log.error(f"Error in World Bank data extraction: {str(e)}")
        raise


@op(
    ins={"wb_data": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Cleaned World Bank data"),
    config_schema=world_bank_config_schema,
    retry_policy=RetryPolicy(max_retries=2, delay=2),
    description="Clean and standardize World Bank data",
    required_resource_keys={"etl_config"},
)
def clean_world_bank_data(
    context,
    wb_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Clean and standardize World Bank data.
    """
    
    if wb_data.empty:
        context.log.warning("No World Bank data to clean")
        return pd.DataFrame()
    
    context.log.info(f"Cleaning {len(wb_data)} World Bank records")
    etl_config: ETLConfigResource = context.resources.etl_config
    
    try:
        cleaned_df = wb_data.copy()
        
        # Remove records with null values
        initial_count = len(cleaned_df)
        cleaned_df = cleaned_df.dropna(subset=['value'])
        context.log.info(f"Removed {initial_count - len(cleaned_df)} records with null values")
        
        # Standardize country names using ETL config mappings
        if hasattr(etl_config, 'country_name_mappings'):
            cleaned_df['country_name_standardized'] = cleaned_df['country_name'].map(
                etl_config.country_name_mappings
            ).fillna(cleaned_df['country_name'])
        else:
            cleaned_df['country_name_standardized'] = cleaned_df['country_name']
        
        # Filter for reasonable data ranges
        cleaned_df = _apply_data_validation(cleaned_df, context)
        
        # Add data quality flags
        cleaned_df['data_quality_score'] = _calculate_data_quality_score(cleaned_df)
        
        context.log.info(f"Cleaned World Bank data: {len(cleaned_df)} records remaining")
        
        return cleaned_df
        
    except Exception as e:
        context.log.error(f"Error cleaning World Bank data: {str(e)}")
        raise


@op(
    ins={"cleaned_wb_data": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Country indicators dimension"),
        config_schema=world_bank_config_schema,
    retry_policy=RetryPolicy(max_retries=2, delay=2),
    description="Create country indicators dimension table"
)
def create_dim_country_indicators(
    context,
    cleaned_wb_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Create the DIM_CountryIndicators dimension table.
    
    This dimension contains economic and social indicators for countries
    organized by country and year to support trend analysis.
    """
    
    if cleaned_wb_data.empty:
        context.log.warning("No cleaned World Bank data available")
        return pd.DataFrame()
    
    context.log.info(f"Creating country indicators dimension from {len(cleaned_wb_data)} records")
    
    try:
        # Pivot the data to have indicators as columns
        pivot_df = cleaned_wb_data.pivot_table(
            index=['country_code', 'country_name_standardized', 'year'],
            columns='indicator_code',
            values='value',
            aggfunc='mean'  # In case of duplicates
        ).reset_index()
        
        # Flatten column names
        pivot_df.columns.name = None
          # Rename indicator columns to more readable names
        indicator_mappings = {
            'NY.GDP.PCAP.CD': 'GDP_PER_CAPITA_USD',
            'HD.HCI.OVRL': 'HUMAN_CAPITAL_INDEX',
            'IT.NET.USER.ZS': 'INTERNET_USERS_PERCENT',
            'SH.MED.PHYS.ZS': 'PHYSICIANS_PER_1000',
            'PV.EST': 'POLITICAL_STABILITY_INDEX'
        }
        
        # Rename columns
        pivot_df = pivot_df.rename(columns=indicator_mappings)
        
        # Create dimension key
        pivot_df['INDICATOR_KEY'] = range(1, len(pivot_df) + 1)
        
        # Rename standard columns
        pivot_df = pivot_df.rename(columns={
            'country_code': 'COUNTRY_CODE',
            'country_name_standardized': 'COUNTRY_NAME',
            'year': 'YEAR'
        })
          # Calculate derived indicators (removed GDP_TOTAL calculation since no population data)
        # Could add other derived metrics here if needed
        
        # Add metadata columns
        pivot_df['LAST_UPDATED'] = datetime.now()
        pivot_df['DATA_SOURCE'] = 'World Bank API'
          # Select final columns
        dimension_columns = [
            'INDICATOR_KEY', 'COUNTRY_CODE', 'COUNTRY_NAME', 'YEAR',
            'GDP_PER_CAPITA_USD', 'HUMAN_CAPITAL_INDEX', 'INTERNET_USERS_PERCENT',
            'PHYSICIANS_PER_1000', 'POLITICAL_STABILITY_INDEX',
            'LAST_UPDATED', 'DATA_SOURCE'
        ]
        
        # Keep only columns that exist
        available_columns = [col for col in dimension_columns if col in pivot_df.columns]
        dim_indicators = pivot_df[available_columns]
        
        # Sort by country and year
        dim_indicators = dim_indicators.sort_values(['COUNTRY_NAME', 'YEAR'])
        
        context.log.info(f"Created country indicators dimension with {len(dim_indicators)} records")
        
        return dim_indicators
        
    except Exception as e:
        context.log.error(f"Error creating country indicators dimension: {str(e)}")
        raise


@op(
    ins={"dim_country_indicators": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Load results"),
    config_schema=world_bank_config_schema,
    retry_policy=RetryPolicy(max_retries=3, delay=2),
    description="Load country indicators dimension to database"
)
def load_dim_country_indicators(
    context,
    dim_country_indicators: pd.DataFrame,
    db: DatabaseResource
) -> Dict[str, Any]:
    """
    Load the country indicators dimension to the database.
    """
    
    if dim_country_indicators.empty:
        context.log.warning("No country indicators data to load")
        return {'status': 'skipped', 'reason': 'No data to load'}
    
    context.log.info(f"Loading {len(dim_country_indicators)} country indicator records")
    
    try:
        # Use upsert to handle updates       
        load_result = db.upsert_dataframe(
            dataframe=dim_country_indicators,
            table_name='DIM_CountryIndicators',
            key_columns=['COUNTRY_CODE', 'YEAR'],
            batch_size=context.op_config['batch_size']
        )
        
        context.log.info(f"Successfully loaded country indicators dimension: {load_result}")
        
        return {
            'table_name': 'DIM_CountryIndicators',
            'records_loaded': len(dim_country_indicators),
            'load_timestamp': pd.Timestamp.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        context.log.error(f"Error loading country indicators dimension: {str(e)}")
        raise


def _apply_data_validation(df: pd.DataFrame, context) -> pd.DataFrame:
    """
    Apply data validation rules to World Bank data.
    """
    
    initial_count = len(df)
      # Define reasonable ranges for each indicator
    validation_rules = {
        'NY.GDP.PCAP.CD': (0, 200000),        # GDP per capita
        'HD.HCI.OVRL': (0, 1),                # Human Capital Index (0-1 scale)
        'IT.NET.USER.ZS': (0, 100),           # Internet users percentage
        'SH.MED.PHYS.ZS': (0, 100),           # Physicians per 1000 people
        'PV.EST': (-3, 3),                    # Political Stability Index (-2.5 to 2.5 scale)
    }
    
    # Apply validation rules
    for indicator, (min_val, max_val) in validation_rules.items():
        if indicator in df['indicator_code'].values:
            mask = (
                (df['indicator_code'] == indicator) & 
                ((df['value'] < min_val) | (df['value'] > max_val))
            )
            df = df[~mask]
    
    removed_count = initial_count - len(df)
    if removed_count > 0:
        context.log.warning(f"Removed {removed_count} records due to validation rules")
    
    return df


def _calculate_data_quality_score(df: pd.DataFrame) -> pd.Series:
    """
    Calculate a data quality score for World Bank records.
    """
    
    # Start with base score
    scores = pd.Series(1.0, index=df.index)
    
    # Penalize very old data
    current_year = datetime.now().year
    age_penalty = np.maximum(0, (current_year - df['year'] - 5) * 0.01)
    scores -= age_penalty
    
    # Ensure scores are between 0 and 1
    scores = scores.clip(0, 1)
    
    return scores
