"""
Himalayan Expeditions ETL Pipeline

This package implements a comprehensive ETL pipeline for processing
Himalayan expeditions data and World Bank indicators into a dimensional
data warehouse using the Dagster framework.
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from dagster import Definitions

from .resources import (
    database_resource,
    filesystem_resource, 
    etl_config_resource,
)

from .jobs import (
    extraction_job,
    dimension_loading_job,
    fact_loading_job,
    full_etl_pipeline,
    incremental_etl_pipeline,
)

from .assets import (
    himalayan_raw_data,
    world_bank_raw_data,
    cleaned_expeditions,
    cleaned_members,
    cleaned_peaks,
    dim_date,
    dim_nationality,
    dim_peak,
    fact_expeditions,
)

# Dagster definitions - this is the main entry point for the pipeline
defs = Definitions(
    assets=[
        himalayan_raw_data,
        world_bank_raw_data,
        cleaned_expeditions,
        cleaned_members,
        cleaned_peaks,
        dim_date,
        dim_nationality,
        dim_peak,
        fact_expeditions,
    ],
    jobs=[
        extraction_job,
        dimension_loading_job,
        fact_loading_job,
        full_etl_pipeline,
        incremental_etl_pipeline,
    ],    resources={
        "mssql": database_resource,
        "filesystem": filesystem_resource,
        "etl_config": etl_config_resource,
    },
)
