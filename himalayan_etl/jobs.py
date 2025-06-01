from dagster import job
from himalayan_etl.ops.extract import (
    extract_world_bank_data,
    extract_expeditions_data,
    extract_members_data,
    extract_peaks_data,
)
from himalayan_etl.ops.transform import (
    transform_expeditions_data,
    transform_members_data,
    transform_peaks_data,
    transform_world_bank_data,
    create_dim_date,
)
from himalayan_etl.ops.load import (
    drop_all_fk,
    load_dim_date,
    load_dim_country_indicator,
    load_dim_peak,
    load_dim_expedition,
    load_dim_route,
    load_fact_member_expedition,
)


@job(
    name="himalayan_etl",
    description="ETL pipeline for Himalayan expeditions data warehouse",
)
def himalayan_etl():
    raw_expeditions = extract_expeditions_data()
    raw_members = extract_members_data()
    raw_peaks = extract_peaks_data()
    raw_world_bank = extract_world_bank_data()

    dim_expedition = transform_expeditions_data(raw_expeditions)
    dim_peak = transform_peaks_data(raw_peaks)
    dim_country_indicator = transform_world_bank_data(raw_world_bank)
    dim_date = create_dim_date(raw_members)
    fact_member_expedition = transform_members_data(raw_members, dim_country_indicator, dim_date, dim_expedition)

    dropped = drop_all_fk(_after=fact_member_expedition)

    dim_expedition_loaded = load_dim_expedition(dim_expedition, _after=dropped)
    dim_peak_loaded = load_dim_peak(dim_peak, _after=dim_expedition_loaded)
    dim_world_bank_loaded = load_dim_country_indicator(dim_country_indicator, _after=dim_peak_loaded)
    dim_date_loaded = load_dim_date(dim_date, _after=dim_world_bank_loaded)
    fact_member_expedition_loaded = load_fact_member_expedition(fact_member_expedition, _after=dim_date_loaded)
