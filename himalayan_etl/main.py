from dagster import Definitions, RetryPolicy, resource

from himalayan_etl import jobs
from himalayan_etl.resources import (
    DatabaseResource,
    FileSystemResource,
    ETLConfigResource,
    WorldBankConfig,
)


def create_dagster_definitions():
    @resource
    def database_resource():
        return DatabaseResource()

    @resource
    def filesystem_resource():
        return FileSystemResource()

    @resource
    def world_bank_config_resource():
        return WorldBankConfig(
            base_url="https://api.worldbank.org/v2",
            start_year=1960,
            end_year=2023,
            indicators=[
                "NY.GDP.PCAP.CD",  # GDP per capita (current US$)
                "HD.HCI.OVRL",  # Human Capital Index (HCI) overall
                "IT.NET.USER.ZS",  # Individuals using the Internet (% of population)
                "SH.MED.PHYS.ZS",  # Physicians (per 1,000 people)
                "PV.EST",  # Political Stability and Absence of Violence
            ],
            timeout=10,
            max_page_size=32767,
        )

    @resource
    def etl_config_resource():
        return ETLConfigResource(
            data_directory="./data",
            log_level="INFO",
        )

    job_definitions = [
        jobs.himalayan_etl,
    ]

    resource_definitions = {
        "db": database_resource,
        "fs": filesystem_resource,
        "etl_config": etl_config_resource,
        "world_bank_config": world_bank_config_resource,
    }

    return Definitions(jobs=job_definitions, resources=resource_definitions)


defs = create_dagster_definitions()
