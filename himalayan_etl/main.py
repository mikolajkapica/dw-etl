"""
Dagster definitions for the Himalayan Expeditions ETL pipeline.
"""

from dagster import Definitions, resource

# Import modules
from himalayan_etl import jobs
from himalayan_etl.resources import (
    DatabaseResource,
    FileSystemResource,
    ETLConfigResource,
)

def create_dagster_definitions() -> Definitions:
    """Create Dagster definitions with all resources and jobs."""
    
    # Define resources with proper Dagster resource decoration
    @resource
    def database_resource():
        return DatabaseResource()
    
    @resource  
    def filesystem_resource():
        return FileSystemResource()
    
    @resource
    def etl_config_resource():
        return ETLConfigResource()
    
    resources = {
        "db": database_resource,
        "fs": filesystem_resource,
        "etl_config": etl_config_resource,
    }

    # Simple job list - remove complex scheduling for now
    all_jobs = [
        jobs.himalayan_etl_full_load,
    ]

    return Definitions(
        jobs=all_jobs,
        resources=resources,
    )


# Dagster definitions object that Dagster looks for
defs = create_dagster_definitions()
