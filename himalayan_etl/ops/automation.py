"""
Automation and incremental loading operations for the Himalayan ETL pipeline.
Handles fuzzy matching, change detection, and incremental updates.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple, Optional, Set
from dagster import op, In, Out, RetryPolicy, Field, Int, Bool
from datetime import datetime, timedelta
import hashlib
from thefuzz import fuzz, process
import sqlalchemy as sa

from himalayan_etl.resources import DatabaseResource, FileSystemResource, ETLConfigResource

logger = logging.getLogger(__name__)


# Configuration schema for automation operations
automation_config_schema = {
    "fuzzy_match_threshold": Field(Int, default_value=85),
    "batch_size": Field(Int, default_value=5000),
    "lookback_days": Field(Int, default_value=30),
    "enable_fuzzy_matching": Field(Bool, default_value=True),
    "max_fuzzy_candidates": Field(Int, default_value=5)
}


@op(
    ins={"new_data": In(pd.DataFrame), "existing_data": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Incremental changes detected"),
    config_schema=automation_config_schema,
    retry_policy=RetryPolicy(max_retries=2, delay=3),
    description="Detect incremental changes in expedition data"
)
def detect_incremental_changes(
    context,
    new_data: pd.DataFrame,
    existing_data: pd.DataFrame,
    etl_config: ETLConfigResource
) -> pd.DataFrame:
    """
    Detect incremental changes between new and existing expedition data.
    
    This op identifies:
    - New expeditions not in the existing dataset
    - Modified expeditions with different data
    - Deleted expeditions (optional)
    """
    
    context.log.info(f"Detecting changes between {len(new_data)} new and {len(existing_data)} existing records")
    
    try:
        # Ensure both datasets have the key column
        key_column = 'EXPID'
        if key_column not in new_data.columns or key_column not in existing_data.columns:
            context.log.error(f"Key column {key_column} not found in datasets")
            return pd.DataFrame()
        
        # Create content hashes for comparison
        new_data = _add_content_hash(new_data, context)
        existing_data = _add_content_hash(existing_data, context)
        
        # Find new records
        new_ids = set(new_data[key_column]) - set(existing_data[key_column])
        new_records = new_data[new_data[key_column].isin(new_ids)].copy()
        new_records['CHANGE_TYPE'] = 'INSERT'
        
        # Find potentially modified records
        common_ids = set(new_data[key_column]) & set(existing_data[key_column])
        
        modified_records = []
        for exp_id in common_ids:
            new_hash = new_data[new_data[key_column] == exp_id]['CONTENT_HASH'].iloc[0]
            existing_hash = existing_data[existing_data[key_column] == exp_id]['CONTENT_HASH'].iloc[0]
            
            if new_hash != existing_hash:
                modified_record = new_data[new_data[key_column] == exp_id].copy()
                modified_record['CHANGE_TYPE'] = 'UPDATE'
                modified_records.append(modified_record)
        
        # Combine all changes
        changes = [new_records]
        if modified_records:
            changes.extend(modified_records)
        
        if changes:
            incremental_df = pd.concat(changes, ignore_index=True)
        else:
            incremental_df = pd.DataFrame()
        
        # Add metadata
        if not incremental_df.empty:
            incremental_df['DETECTED_AT'] = datetime.now()
            incremental_df['BATCH_ID'] = f"incr_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        context.log.info(f"Detected {len(new_records)} new and {len(modified_records)} modified records")
        
        return incremental_df
        
    except Exception as e:
        context.log.error(f"Error detecting incremental changes: {str(e)}")
        raise


@op(
    ins={"members_data": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Members data with fuzzy matched names"),
    config_schema=automation_config_schema,
    retry_policy=RetryPolicy(max_retries=2, delay=3),
    description="Apply fuzzy matching to standardize member names"
)
def apply_fuzzy_name_matching(
    context,
    members_data: pd.DataFrame,
    etl_config: ETLConfigResource
) -> pd.DataFrame:
    """
    Apply fuzzy matching to standardize member names and identify potential duplicates.
    
    This op helps consolidate member records that may have slight name variations
    due to different transliterations, spellings, or data entry errors.
    """
    if not context.op_config["enable_fuzzy_matching"]:
        context.log.info("Fuzzy matching disabled - returning original data")
        return members_data
    
    context.log.info(f"Applying fuzzy name matching to {len(members_data)} member records")
    
    try:
        df = members_data.copy()
        
        if 'MEMBER_NAME' not in df.columns:
            context.log.warning("MEMBER_NAME column not found - skipping fuzzy matching")
            return df
        
        # Clean names for better matching
        df['CLEAN_NAME'] = df['MEMBER_NAME'].str.strip().str.upper()
        df['CLEAN_NAME'] = df['CLEAN_NAME'].str.replace(r'[^\w\s]', '', regex=True)
        
        # Get unique names for matching
        unique_names = df['CLEAN_NAME'].dropna().unique()
        context.log.info(f"Processing {len(unique_names)} unique names")
        
        # Create name mapping using fuzzy matching
        name_mapping = {}
        processed_names = set()
        
        for i, name in enumerate(unique_names):
            if name in processed_names:
                continue
            # Find similar names
            matches = process.extract(
                name, 
                unique_names, 
                scorer=fuzz.ratio,
                limit=context.op_config["max_fuzzy_candidates"]
            )
            
            # Group names that meet the threshold
            similar_group = [name]
            for match_name, score in matches:
                if match_name != name and score >= context.op_config["fuzzy_match_threshold"]:
                    similar_group.append(match_name)
                    processed_names.add(match_name)
            
            # Use the most common name as the canonical form
            if len(similar_group) > 1:
                # Count occurrences to find most common
                name_counts = df[df['CLEAN_NAME'].isin(similar_group)]['CLEAN_NAME'].value_counts()
                canonical_name = name_counts.index[0]
                
                # Map all similar names to canonical name
                for similar_name in similar_group:
                    name_mapping[similar_name] = canonical_name
                
                context.log.debug(f"Grouped {len(similar_group)} names under '{canonical_name}'")
            
            processed_names.add(name)
        
        # Apply name mapping
        df['STANDARDIZED_NAME'] = df['CLEAN_NAME'].map(name_mapping).fillna(df['CLEAN_NAME'])
        
        # Add fuzzy matching metadata
        df['NAME_STANDARDIZED'] = df['CLEAN_NAME'] != df['STANDARDIZED_NAME']
        df['FUZZY_MATCH_APPLIED'] = datetime.now()
        
        standardized_count = df['NAME_STANDARDIZED'].sum()
        context.log.info(f"Standardized {standardized_count} names using fuzzy matching")
        
        return df
        
    except Exception as e:
        context.log.error(f"Error in fuzzy name matching: {str(e)}")
        raise


@op(
    ins={"source_data": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Data quality metrics"),
    config_schema=automation_config_schema,
    retry_policy=RetryPolicy(max_retries=2, delay=2),
    description="Calculate comprehensive data quality metrics"
)
def calculate_data_quality_metrics(
    context,
    source_data: pd.DataFrame
) -> Dict[str, Any]:
    """
    Calculate comprehensive data quality metrics for monitoring and alerting.
    """
    
    context.log.info(f"Calculating data quality metrics for {len(source_data)} records")
    
    try:
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'record_count': len(source_data),
            'column_count': len(source_data.columns),
            'completeness': {},
            'validity': {},
            'consistency': {},
            'uniqueness': {},
            'overall_score': 0.0
        }
        
        if source_data.empty:
            context.log.warning("Empty dataset provided for quality assessment")
            return metrics
        
        # Completeness metrics
        for column in source_data.columns:
            null_count = source_data[column].isnull().sum()
            completeness = 1.0 - (null_count / len(source_data))
            metrics['completeness'][column] = round(completeness, 4)
        
        # Validity metrics (basic type checking)
        if 'YEAR' in source_data.columns:
            valid_years = source_data['YEAR'].between(1900, 2030, na=False).sum()
            metrics['validity']['year_validity'] = round(valid_years / len(source_data), 4)
        
        if 'AGE' in source_data.columns:
            valid_ages = source_data['AGE'].between(10, 100, na=False).sum()
            metrics['validity']['age_validity'] = round(valid_ages / len(source_data), 4)
        
        # Uniqueness metrics
        if 'EXPID' in source_data.columns:
            unique_expids = source_data['EXPID'].nunique()
            metrics['uniqueness']['expid_uniqueness'] = round(unique_expids / len(source_data), 4)
        
        if 'MEMBER_ID' in source_data.columns:
            unique_members = source_data['MEMBER_ID'].nunique()
            metrics['uniqueness']['member_uniqueness'] = round(unique_members / len(source_data), 4)
        
        # Consistency metrics
        if 'SUCCESS' in source_data.columns and 'DEATH' in source_data.columns:
            # Check for logical inconsistencies (success and death at same time)
            inconsistent = ((source_data['SUCCESS'] == 1) & (source_data['DEATH'] == 1)).sum()
            metrics['consistency']['success_death_consistency'] = round(1.0 - (inconsistent / len(source_data)), 4)
        
        # Calculate overall score
        all_scores = []
        for category in ['completeness', 'validity', 'uniqueness', 'consistency']:
            if metrics[category]:
                category_scores = list(metrics[category].values())
                all_scores.extend(category_scores)
        
        if all_scores:
            metrics['overall_score'] = round(sum(all_scores) / len(all_scores), 4)
        
        context.log.info(f"Data quality assessment complete. Overall score: {metrics['overall_score']:.2%}")
        
        return metrics
        
    except Exception as e:
        context.log.error(f"Error calculating data quality metrics: {str(e)}")
        raise


@op(
    ins={"incremental_data": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Load results"),
    config_schema=automation_config_schema,
    retry_policy=RetryPolicy(max_retries=3, delay=5),
    description="Load incremental data with conflict resolution"
)
def load_incremental_data(
    context,
    incremental_data: pd.DataFrame,
    db: DatabaseResource
) -> Dict[str, Any]:
    """
    Load incremental data to the database with conflict resolution.
    
    This op handles INSERT and UPDATE operations based on the CHANGE_TYPE
    column and provides detailed logging of the load process.
    """
    
    context.log.info(f"Loading {len(incremental_data)} incremental records")
    
    if incremental_data.empty:
        return {'status': 'success', 'message': 'No incremental data to load'}
    
    try:
        # Group by change type
        inserts = incremental_data[incremental_data['CHANGE_TYPE'] == 'INSERT'] if 'CHANGE_TYPE' in incremental_data.columns else incremental_data
        updates = incremental_data[incremental_data['CHANGE_TYPE'] == 'UPDATE'] if 'CHANGE_TYPE' in incremental_data.columns else pd.DataFrame()
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'inserts_attempted': len(inserts),
            'updates_attempted': len(updates),
            'inserts_successful': 0,
            'updates_successful': 0,
            'errors': []
        }
        
        # Handle inserts
        if not inserts.empty:
            try:                # Remove metadata columns before insert
                insert_df = inserts.drop(columns=[col for col in ['CHANGE_TYPE', 'DETECTED_AT', 'BATCH_ID', 'CONTENT_HASH'] if col in inserts.columns])
                insert_result = db.bulk_insert_dataframe(
                    dataframe=insert_df,
                    table_name='FACT_Expeditions',  # This would be determined dynamically
                    batch_size=context.op_config["batch_size"]
                )
                
                results['inserts_successful'] = len(inserts)
                context.log.info(f"Successfully inserted {len(inserts)} records")
                
            except Exception as e:
                error_msg = f"Error inserting records: {str(e)}"
                results['errors'].append(error_msg)
                context.log.error(error_msg)
        
        # Handle updates
        if not updates.empty:
            try:
                # Remove metadata columns before update
                update_df = updates.drop(columns=[col for col in ['CHANGE_TYPE', 'DETECTED_AT', 'BATCH_ID', 'CONTENT_HASH'] if col in updates.columns])
                
                update_result = db.upsert_dataframe(
                                    dataframe=update_df,
                    table_name='FACT_Expeditions',  # This would be determined dynamically
                    key_columns=['EXPID'],
                    batch_size=context.op_config["batch_size"]
                )
                
                results['updates_successful'] = len(updates)
                context.log.info(f"Successfully updated {len(updates)} records")
                
            except Exception as e:
                error_msg = f"Error updating records: {str(e)}"
                results['errors'].append(error_msg)
                context.log.error(error_msg)
        
        # Log ETL control information
        try:
            _log_etl_batch(db, results, context)
        except Exception as e:
            context.log.warning(f"Failed to log ETL batch info: {str(e)}")
        
        results['status'] = 'success' if not results['errors'] else 'partial_success'
        
        return results
        
    except Exception as e:
        context.log.error(f"Error in incremental load: {str(e)}")
        raise


@op(
    out=Out(Dict[str, Any], description="System health metrics"),
    config_schema=automation_config_schema,
    retry_policy=RetryPolicy(max_retries=2, delay=3),
    description="Monitor system health and performance"
)
def monitor_system_health(
    context,
    db: DatabaseResource
) -> Dict[str, Any]:
    """
    Monitor system health including database connectivity, table sizes, and ETL performance.
    """
    
    context.log.info("Monitoring system health")
    
    try:
        health_metrics = {
            'timestamp': datetime.now().isoformat(),
            'database_connectivity': False,
            'table_sizes': {},
            'etl_performance': {},
            'alerts': [],
            'overall_status': 'unknown'
        }
        
        # Test database connectivity
        try:
            test_query = "SELECT 1 as test"
            db.execute_query(test_query)
            health_metrics['database_connectivity'] = True
        except Exception as e:
            health_metrics['alerts'].append(f"Database connectivity issue: {str(e)}")
        
        # Check table sizes
        if health_metrics['database_connectivity']:
            try:
                size_queries = {
                    'FACT_Expeditions': "SELECT COUNT(*) as count FROM FACT_Expeditions",
                    'DIM_Member': "SELECT COUNT(*) as count FROM DIM_Member",
                    'DIM_Peak': "SELECT COUNT(*) as count FROM DIM_Peak"
                }
                
                for table_name, query in size_queries.items():
                    try:
                        result = db.execute_query(query)
                        health_metrics['table_sizes'][table_name] = result[0]['count'] if result else 0
                    except Exception as e:
                        health_metrics['alerts'].append(f"Failed to check {table_name} size: {str(e)}")
                        
            except Exception as e:
                health_metrics['alerts'].append(f"Error checking table sizes: {str(e)}")
        
        # Check recent ETL performance
        try:
            perf_query = """
                SELECT TOP 5 
                    batch_id, 
                    start_time, 
                    end_time, 
                    records_processed,
                    DATEDIFF(minute, start_time, end_time) as duration_minutes
                FROM ETL_BatchLog 
                ORDER BY start_time DESC
            """
            
            recent_batches = db.execute_query(perf_query)
            health_metrics['etl_performance']['recent_batches'] = recent_batches or []
            
            # Calculate average performance
            if recent_batches:
                avg_duration = sum([b.get('duration_minutes', 0) for b in recent_batches]) / len(recent_batches)
                health_metrics['etl_performance']['avg_duration_minutes'] = round(avg_duration, 2)
                
        except Exception as e:
            health_metrics['alerts'].append(f"Failed to check ETL performance: {str(e)}")
        
        # Determine overall status
        if not health_metrics['alerts']:
            health_metrics['overall_status'] = 'healthy'
        elif health_metrics['database_connectivity']:
            health_metrics['overall_status'] = 'warning'
        else:
            health_metrics['overall_status'] = 'critical'
        
        context.log.info(f"System health check complete. Status: {health_metrics['overall_status']}")
        
        return health_metrics
        
    except Exception as e:
        context.log.error(f"Error monitoring system health: {str(e)}")
        raise


def _add_content_hash(df: pd.DataFrame, context) -> pd.DataFrame:
    """
    Add a content hash column to detect changes in records.
    """
    try:
        # Convert relevant columns to string and concatenate
        hash_columns = [col for col in df.columns if col not in ['DETECTED_AT', 'BATCH_ID', 'CONTENT_HASH']]
        
        df['CONTENT_HASH'] = df[hash_columns].apply(
            lambda row: hashlib.md5('|'.join(row.astype(str)).encode()).hexdigest(),
            axis=1
        )
        
        return df
        
    except Exception as e:
        context.log.warning(f"Failed to add content hash: {str(e)}")
        df['CONTENT_HASH'] = 'unknown'
        return df


def _log_etl_batch(db: DatabaseResource, results: Dict[str, Any], context) -> None:
    """
    Log ETL batch information to the control table.
    """
    try:
        batch_log = pd.DataFrame([{
            'batch_id': f"incr_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'start_time': datetime.now(),
            'end_time': datetime.now(),
            'records_processed': results['inserts_successful'] + results['updates_successful'],
            'inserts_count': results['inserts_successful'],
            'updates_count': results['updates_successful'],
            'errors_count': len(results['errors']),
            'status': results['status']
        }])
        
        db.bulk_insert_dataframe(
            dataframe=batch_log,
            table_name='ETL_BatchLog'
        )
        
    except Exception as e:
        context.log.warning(f"Failed to log batch information: {str(e)}")
