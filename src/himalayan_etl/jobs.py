"""
Dagster jobs for orchestrating the Himalayan Expeditions ETL process.
Defines the complete data pipeline workflow.
"""

import logging
from dagster import (
    job, 
    op, 
    In,    Out, 
    Config, 
    RetryPolicy,
    DefaultSensorStatus,
    sensor,
    RunRequest,
    SkipReason,
    schedule,
    ScheduleEvaluationContext
)
from typing import Dict, Any
import os
from datetime import datetime, timedelta

from himalayan_etl.resources import DatabaseResource, FileSystemResource, ETLConfigResource

# Import all operation modules
from himalayan_etl.ops.extraction import (
    extract_expeditions_data,
    extract_members_data, 
    extract_peaks_data,
    extract_references_data,
    extract_world_bank_data,
    validate_extracted_data
)

from himalayan_etl.ops.cleaning import (
    clean_expeditions_data,
    clean_members_data,
    clean_peaks_data,
    clean_world_bank_data
)

from himalayan_etl.ops.dimensions import (
    create_dim_date,
    create_dim_nationality,
    create_dim_peak,
    create_dim_route,
    create_dim_expedition_status,
    create_dim_host_country,
    create_dim_member,
    load_dimension_table
)

from himalayan_etl.ops.facts import (
    prepare_fact_expeditions,
    prepare_bridge_expedition_members,
    load_fact_expeditions,
    load_bridge_expedition_members
)

from himalayan_etl.ops.world_bank import (
    extract_world_bank_data,
    clean_world_bank_data,
    create_dim_country_indicators,
    load_dim_country_indicators
)

logger = logging.getLogger(__name__)


class ETLJobConfig(Config):
    """Configuration for ETL jobs."""
    data_path: str = "data/"
    load_world_bank_data: bool = True
    incremental_load: bool = False
    enable_data_validation: bool = True
    batch_size: int = 10000


@job(
    name="himalayan_etl_full_load",
    description="Complete ETL pipeline for Himalayan expeditions data warehouse",
    resource_defs={
        "db": DatabaseResource,
        "fs": FileSystemResource,
        "etl_config": ETLConfigResource
    },
    config=ETLJobConfig
)
def himalayan_etl_full_load():
    """
    Main ETL job for complete data warehouse loading.
    
    This job orchestrates the entire ETL process:
    1. Data extraction from CSV files and APIs
    2. Data cleaning and standardization
    3. Dimension table creation and loading
    4. Fact table preparation and loading
    5. Data quality validation
    """
      # Step 1: Data Extraction
    raw_expeditions = extract_expeditions_data()
    raw_members = extract_members_data()
    raw_peaks = extract_peaks_data()
    raw_references = extract_references_data()
    
    # Validate data sources
    validation_results = validate_extracted_data(
        raw_expeditions, raw_members, raw_peaks, raw_references
    )
      # Step 2: Data Cleaning
    cleaned_expeditions = clean_expeditions_data(raw_expeditions)
    cleaned_members = clean_members_data(raw_members)
    cleaned_peaks = clean_peaks_data(raw_peaks)
    # cleaned_references = clean_references_data(raw_references)  # Function not implemented yet
    
    # Text standardization and derived fields will be handled within cleaning functions
    enhanced_expeditions = cleaned_expeditions
    enhanced_members = cleaned_members
      # Step 3: Dimension Creation and Loading
    # Date dimension
    dim_date = create_dim_date(enhanced_expeditions)
    date_load_result = load_dimension_table(dim_date, "DIM_Date")
    
    # Nationality dimension
    dim_nationality = create_dim_nationality(enhanced_members)
    nationality_load_result = load_dimension_table(dim_nationality, "DIM_Nationality")
    
    # Peak dimension
    dim_peak = create_dim_peak(cleaned_peaks, enhanced_expeditions)
    peak_load_result = load_dimension_table(dim_peak, "DIM_Peak")
    
    # Route dimension
    dim_route = create_dim_route(enhanced_expeditions, dim_peak)
    route_load_result = load_dimension_table(dim_route, "DIM_Route")
      # Expedition status dimension
    dim_expedition_status = create_dim_expedition_status(enhanced_expeditions)
    status_load_result = load_dimension_table(dim_expedition_status, "DIM_ExpeditionStatus")
    
    # Host country dimension
    dim_host_country = create_dim_host_country(enhanced_expeditions)
    country_load_result = load_dimension_table(dim_host_country, "DIM_HostCountry")
    
    # Member dimension
    dim_member = create_dim_member(enhanced_members)
    member_load_result = load_dimension_table(dim_member, "DIM_Member")
    
    # Step 4: World Bank Data (Optional)
    wb_data = extract_world_bank_data()
    cleaned_wb_data = clean_world_bank_data(wb_data)
    dim_country_indicators = create_dim_country_indicators(cleaned_wb_data)
    wb_load_result = load_dim_country_indicators(dim_country_indicators)
    
    # Step 5: Fact Table Processing
    fact_expeditions = prepare_fact_expeditions(
        enhanced_expeditions,
        enhanced_members,
        dim_date,
        dim_nationality,
        dim_peak,
        dim_route,
        dim_expedition_status,
        dim_host_country,
        dim_member
    )
    
    bridge_expedition_members = prepare_bridge_expedition_members(
        enhanced_expeditions,
        enhanced_members,
        dim_member
    )
    
    # Step 6: Load Fact and Bridge Tables
    fact_load_result = load_fact_expeditions(fact_expeditions)
    bridge_load_result = load_bridge_expedition_members(bridge_expedition_members)


@job(
    name="himalayan_etl_incremental",
    description="Incremental ETL pipeline for new/updated records",
    resource_defs={
        "db": DatabaseResource,
        "fs": FileSystemResource,
        "etl_config": ETLConfigResource
    },
    config=ETLJobConfig
)
def himalayan_etl_incremental():
    """
    Incremental ETL job for processing only new or updated data.
    
    This job is designed for regular updates when new expedition
    data becomes available.
    """
      # Extract only new/updated records
    raw_expeditions = extract_expeditions_data()
    raw_members = extract_members_data()
    
    # Apply incremental processing logic
    cleaned_expeditions = clean_expeditions_data(raw_expeditions)
    cleaned_members = clean_members_data(raw_members)
      # Process dimensions (with upsert logic)
    dim_member = create_dim_member(cleaned_members)
    member_load_result = load_dimension_table(dim_member, "DIM_Member")
    
    # Process fact tables
    fact_expeditions = prepare_fact_expeditions(
        cleaned_expeditions,
        cleaned_members,
        # Load existing dimensions from database
        None, None, None, None, None, None, dim_member
    )
    
    fact_load_result = load_fact_expeditions(fact_expeditions)


@job(
    name="world_bank_data_refresh",
    description="Refresh World Bank country indicators data",
    resource_defs={
        "db": DatabaseResource,
        "etl_config": ETLConfigResource
    }
)
def world_bank_data_refresh():
    """
    Standalone job for refreshing World Bank data.
    
    This job can be run independently to update economic
    and social indicators for countries.
    """
    
    wb_data = extract_world_bank_data()
    cleaned_wb_data = clean_world_bank_data(wb_data)
    dim_country_indicators = create_dim_country_indicators(cleaned_wb_data)
    wb_load_result = load_dim_country_indicators(dim_country_indicators)


@op(
    out=Out(Dict[str, Any], description="Data quality report"),
    config_schema=ETLJobConfig,
    description="Generate comprehensive data quality report"
)
def generate_data_quality_report(
    context,
    config: ETLJobConfig,
    db: DatabaseResource
) -> Dict[str, Any]:
    """
    Generate a comprehensive data quality report for the data warehouse.
    """
    
    context.log.info("Generating data quality report")
    
    try:
        # Query data warehouse for quality metrics
        quality_queries = {
            'total_expeditions': "SELECT COUNT(*) as count FROM FACT_Expeditions",
            'total_members': "SELECT COUNT(*) as count FROM BRIDGE_ExpeditionMembers",
            'date_coverage': """
                SELECT MIN(YEAR) as min_year, MAX(YEAR) as max_year, COUNT(DISTINCT YEAR) as year_count
                FROM FACT_Expeditions
            """,
            'peak_coverage': "SELECT COUNT(DISTINCT PEAK_KEY) as unique_peaks FROM FACT_Expeditions",
            'success_rate': """
                SELECT AVG(SUCCESS_RATE) as avg_success_rate,
                       MIN(SUCCESS_RATE) as min_success_rate,
                       MAX(SUCCESS_RATE) as max_success_rate
                FROM FACT_Expeditions
                WHERE TOTAL_MEMBERS > 0
            """,
            'data_completeness': """
                SELECT 
                    COUNT(*) as total_records,
                    SUM(CASE WHEN DATE_ID = -1 THEN 1 ELSE 0 END) as missing_dates,
                    SUM(CASE WHEN PEAK_KEY = -1 THEN 1 ELSE 0 END) as missing_peaks,
                    SUM(CASE WHEN COUNTRY_KEY = -1 THEN 1 ELSE 0 END) as missing_countries
                FROM FACT_Expeditions
            """
        }
        
        report = {
            'report_timestamp': datetime.now().isoformat(),
            'database_metrics': {},
            'data_quality_score': 0.0
        }
        
        total_score = 0.0
        metric_count = 0
        
        for metric_name, query in quality_queries.items():
            try:
                result = db.execute_query(query)
                report['database_metrics'][metric_name] = result
                
                # Simple scoring logic
                if metric_name == 'total_expeditions':
                    score = min(1.0, result[0]['count'] / 10000)  # Target 10k+ expeditions
                elif metric_name == 'data_completeness':
                    total = result[0]['total_records']
                    missing = (result[0]['missing_dates'] + 
                              result[0]['missing_peaks'] + 
                              result[0]['missing_countries'])
                    score = max(0.0, 1.0 - (missing / (total * 3)))
                else:
                    score = 1.0
                
                total_score += score
                metric_count += 1
                
            except Exception as e:
                context.log.warning(f"Error executing quality query {metric_name}: {str(e)}")
                report['database_metrics'][metric_name] = {'error': str(e)}
        
        # Calculate overall quality score
        if metric_count > 0:
            report['data_quality_score'] = round(total_score / metric_count, 3)
        
        context.log.info(f"Data quality report generated with score: {report['data_quality_score']}")
        
        return report
        
    except Exception as e:
        context.log.error(f"Error generating data quality report: {str(e)}")
        raise


@job(
    name="data_quality_assessment",
    description="Generate data quality report for the data warehouse",
    resource_defs={
        "db": DatabaseResource,
        "etl_config": ETLConfigResource
    },
    config=ETLJobConfig
)
def data_quality_assessment():
    """
    Job for generating data quality assessment reports.
    """
    
    quality_report = generate_data_quality_report()


# Schedules
@schedule(
    job=himalayan_etl_incremental,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    default_status=DefaultSensorStatus.STOPPED
)
def daily_incremental_schedule(context: ScheduleEvaluationContext):
    """Daily incremental ETL schedule."""
    
    return RunRequest(
        run_key=f"incremental_{context.scheduled_execution_time.strftime('%Y%m%d')}",        run_config={
            "ops": {
                "extract_expeditions_data": {"config": {"incremental_load": True}},
                "extract_members_data": {"config": {"incremental_load": True}}
            }
        }
    )


@schedule(
    job=world_bank_data_refresh,
    cron_schedule="0 3 1 * *",  # Monthly on 1st at 3 AM
    default_status=DefaultSensorStatus.STOPPED
)
def monthly_world_bank_schedule(context: ScheduleEvaluationContext):
    """Monthly World Bank data refresh schedule."""
    
    return RunRequest(
        run_key=f"world_bank_{context.scheduled_execution_time.strftime('%Y%m')}"
    )


@schedule(
    job=data_quality_assessment,
    cron_schedule="0 6 * * 1",  # Weekly on Monday at 6 AM
    default_status=DefaultSensorStatus.STOPPED
)
def weekly_quality_assessment_schedule(context: ScheduleEvaluationContext):
    """Weekly data quality assessment schedule."""
    
    return RunRequest(
        run_key=f"quality_{context.scheduled_execution_time.strftime('%Y%m%d')}"
    )


# File-based sensor for automatic triggering
@sensor(
    job=himalayan_etl_incremental,
    default_status=DefaultSensorStatus.STOPPED
)
def file_update_sensor(context):
    """
    Sensor that triggers incremental ETL when new data files are detected.
    """
    
    # Check for new or modified files
    data_path = os.getenv('DATA_PATH', './data')
    csv_files = ['exped.csv', 'members.csv', 'peaks.csv', 'refer.csv']
    
    try:
        latest_modification = 0
        modified_files = []
        
        for filename in csv_files:
            filepath = os.path.join(data_path, filename)
            if os.path.exists(filepath):
                mtime = os.path.getmtime(filepath)
                if mtime > latest_modification:
                    latest_modification = mtime
                    modified_files.append(filename)
        
        # Check if files were modified in the last hour
        if latest_modification > (datetime.now().timestamp() - 3600):
            context.log.info(f"Detected file updates: {modified_files}")
            return RunRequest(
                run_key=f"file_update_{int(latest_modification)}",
                run_config={
                    "ops": {
                        "validate_data_sources": {
                            "config": {"check_file_timestamps": True}
                        }
                    }
                }
            )
        
        return SkipReason("No recent file updates detected")
        
    except Exception as e:
        context.log.error(f"Error in file update sensor: {str(e)}")
        return SkipReason(f"Sensor error: {str(e)}")
