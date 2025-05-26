"""
Simplified Dagster job for the Himalayan Expeditions ETL process.
"""

from dagster import job
from himalayan_etl.ops.world_bank import extract_world_bank_data, create_dim_country_indicators
from himalayan_etl.resources import DatabaseResource, FileSystemResource, ETLConfigResource

# Import main operation modules
from himalayan_etl.ops.extraction import (
    extract_expeditions_data,
    extract_members_data, 
    extract_peaks_data,
)

from himalayan_etl.ops.cleaning import (
    clean_expeditions_data,
    clean_members_data,
    clean_peaks_data,
    clean_world_bank_data,
)

from himalayan_etl.ops.dimensions import (
    create_dim_date,
    create_dim_nationality,
    create_dim_peak,
    create_dim_route,
    create_dim_expedition_status,
    create_dim_host_country,
    create_dim_member,
    load_dimension_table,
)

from himalayan_etl.ops.facts import (
    prepare_fact_expeditions,
    load_fact_expeditions,
)

# Create configured dimension loading ops for each table
load_dim_date = load_dimension_table.configured(
    {"table_name": "DIM_Date", "key_columns": ["Date"]},
    name="load_dim_date"
)

load_dim_nationality = load_dimension_table.configured(
    {"table_name": "DIM_Nationality", "key_columns": ["CountryCode"]},
    name="load_dim_nationality"
)

load_dim_peak = load_dimension_table.configured(
    {"table_name": "DIM_Peak", "key_columns": ["PeakID"]},
    name="load_dim_peak"
)

load_dim_route = load_dimension_table.configured(
    {"table_name": "DIM_Route", "key_columns": ["Route1", "Route2"]},
    name="load_dim_route"
)

load_dim_expedition_status = load_dimension_table.configured(
    {"table_name": "DIM_ExpeditionStatus", "key_columns": ["TerminationReason"]},
    name="load_dim_expedition_status"
)

load_dim_host_country = load_dimension_table.configured(
    {"table_name": "DIM_HostCountry", "key_columns": ["CountryCode"]},
    name="load_dim_host_country"
)

load_dim_member = load_dimension_table.configured(
    {"table_name": "DIM_Member", "key_columns": ["MemberID"]},
    name="load_dim_member"
)

load_dim_country_indicators = load_dimension_table.configured(
    {"table_name": "DIM_CountryIndicators", "key_columns": ["CountryCode", "IndicatorCode", "Year"]},
    name="load_dim_country_indicators"
)

load_dim_country_indicators = load_dimension_table.configured(
    {"table_name": "DIM_CountryIndicators", "key_columns": ["CountryCode", "IndicatorCode", "Year"]},
    name="load_dim_country_indicators"
)


@job(
    name="himalayan_etl_full_load",
    description="Complete ETL pipeline for Himalayan expeditions data warehouse",
)
def himalayan_etl_full_load():
    raw_expeditions = extract_expeditions_data()
    raw_members = extract_members_data()
    raw_peaks = extract_peaks_data()
    raw_world_bank = extract_world_bank_data()

    cleaned_expeditions = clean_expeditions_data(raw_expeditions)
    cleaned_members = clean_members_data(raw_members)
    cleaned_peaks = clean_peaks_data(raw_peaks)
    cleaned_world_bank = clean_world_bank_data(raw_world_bank)    

    dim_date = create_dim_date(cleaned_expeditions)
    dim_nationality = create_dim_nationality(cleaned_expeditions, cleaned_members)
    dim_peak = create_dim_peak(cleaned_peaks, cleaned_expeditions)
    dim_route = create_dim_route(cleaned_expeditions)
    dim_expedition_status = create_dim_expedition_status(cleaned_expeditions)
    dim_host_country = create_dim_host_country(cleaned_expeditions)
    dim_member = create_dim_member(cleaned_members)
    
    # Create World Bank indicators dimension
    dim_country_indicators = create_dim_country_indicators(cleaned_world_bank)    # Load all dimensions first
    dim_date_loaded = load_dim_date(dim_date)
    dim_nationality_loaded = load_dim_nationality(dim_nationality)
    dim_peak_loaded = load_dim_peak(dim_peak) 
    dim_route_loaded = load_dim_route(dim_route)
    dim_expedition_status_loaded = load_dim_expedition_status(dim_expedition_status)
    dim_host_country_loaded = load_dim_host_country(dim_host_country)
    dim_member_loaded = load_dim_member(dim_member)
    dim_country_indicators_loaded = load_dim_country_indicators(dim_country_indicators)
    
    # Prepare fact table (depends on dimension data, not loading results)
    fact_expeditions = prepare_fact_expeditions(
        cleaned_expeditions,
        cleaned_members,
        dim_date,
        dim_nationality,
        dim_peak,
        dim_route,
        dim_expedition_status,
        dim_host_country,
        dim_member
    )
    
    # Load facts only after all dimensions are loaded
    load_fact_expeditions(
        fact_expeditions,
        dim_date_loaded,
        dim_nationality_loaded,
        dim_peak_loaded,
        dim_route_loaded,
        dim_expedition_status_loaded,
        dim_host_country_loaded,
        dim_member_loaded,
        dim_country_indicators_loaded
    )
