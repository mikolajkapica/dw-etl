"""
Fact table operations for the Himalayan Expeditions ETL process.
Handles FACT_Expeditions table processing with member-centric records.
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Tuple
from dagster import op, In, Out, RetryPolicy, DagsterLogManager, Field, Int, Bool

from himalayan_etl.resources import DatabaseResource, ETLConfigResource

logger = logging.getLogger(__name__)


# Configuration schema for fact table operations
fact_config_schema = {
    "batch_size": Field(Int, default_value=10000),
    "enable_validation": Field(Bool, default_value=True),
    "min_year": Field(Int, default_value=1900),
    "max_year": Field(Int, default_value=2050)
}


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
    config_schema=fact_config_schema,
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
    dim_member: pd.DataFrame
) -> pd.DataFrame:

    # print sample of cleaned expeditions
    context.log.info(f"cleaned_expeditions: {list(cleaned_expeditions.columns)}")
    context.log.info(f"cleaned_members: {list(cleaned_members.columns)}")
    context.log.info(f"dim_date: {list(dim_date.columns)}")
    context.log.info(f"dim_nationality: {list(dim_nationality.columns)}")
    context.log.info(f"dim_peak: {list(dim_peak.columns)}")
    context.log.info(f"dim_route: {list(dim_route.columns)}")
    context.log.info(f"dim_expedition_status: {list(dim_expedition_status.columns)}")
    context.log.info(f"dim_host_country: {list(dim_host_country.columns)}")
    context.log.info(f"dim_member: {list(dim_member.columns)}")

    # cleaned_expeditions: ['EXPID', 'PEAKID', 'YEAR', 'SEASON', 'HOST', 'ROUTE1', 'ROUTE2', 'ROUTE3', 'ROUTE4', 'NATION', 'LEADERS', 'SPONSOR', 'SUCCESS1', 'SUCCESS2', 'SUCCESS3', 'SUCCESS4', 'ASCENT1', 'ASCENT2', 'ASCENT3', 'ASCENT4', 'CLAIMED', 'DISPUTED', 'COUNTRIES', 'APPROACH', 'BCDATE', 'SMTDATE', 'SMTTIME', 'SMTDAYS', 'TOTDAYS', 'TERMDATE', 'TERMREASON', 'TERMNOTE', 'HIGHPOINT', 'TRAVERSE', 'SKI', 'PARAPENTE', 'CAMPS', 'ROPE', 'TOTMEMBERS', 'SMTMEMBERS', 'MDEATHS', 'TOTHIRED', 'SMTHIRED', 'HDEATHS', 'NOHIRED', 'O2USED', 'O2NONE', 'O2CLIMB', 'O2DESCENT', 'O2SLEEP', 'O2MEDICAL', 'O2TAKEN', 'O2UNKWN', 'OTHERSMTS', 'CAMPSITES', 'ROUTEMEMO', 'ACCIDENTS', 'ACHIEVMENT', 'AGENCY', 'COMRTE', 'STDRTE', 'PRIMRTE', 'PRIMMEM', 'PRIMREF', 'PRIMID', 'CHKSUM', 'LEADERS_LIST', 'LEADER_COUNT', 'IS_SUCCESS', 'TOTAL_DEATHS']
    # cleaned_members: ['EXPID', 'MEMBID', 'PEAKID', 'MYEAR', 'MSEASON', 'FNAME', 'LNAME', 'SEX', 'AGE', 'BIRTHDATE', 'YOB', 'CALCAGE', 'CITIZEN', 'STATUS', 'RESIDENCE', 'OCCUPATION', 'LEADER', 'DEPUTY', 'BCONLY', 'NOTTOBC', 'SUPPORT', 'DISABLED', 'HIRED', 'SHERPA', 'TIBETAN', 'MSUCCESS', 'MCLAIMED', 'MDISPUTED', 'MSOLO', 'MTRAVERSE', 'MSKI', 'MPARAPENTE', 'MSPEED', 'MHIGHPT', 'MPERHIGHPT', 'MSMTDATE1', 'MSMTDATE2', 'MSMTDATE3', 'MSMTTIME1', 'MSMTTIME2', 'MSMTTIME3', 'MROUTE1', 'MROUTE2', 'MROUTE3', 'MASCENT1', 'MASCENT2', 'MASCENT3', 'MO2USED', 'MO2NONE', 'MO2CLIMB', 'MO2DESCENT', 'MO2SLEEP', 'MO2MEDICAL', 'MO2NOTE', 'DEATH', 'DEATHDATE', 'DEATHTIME', 'DEATHTYPE', 'DEATHHGTM', 'DEATHCLASS', 'AMS', 'WEATHER', 'INJURY', 'INJURYDATE', 'INJURYTIME', 'INJURYTYPE', 'INJURYHGTM', 'DEATHNOTE', 'MEMBERMEMO', 'NECROLOGY', 'MSMTBID', 'MSMTTERM', 'HCN', 'MCHKSUM', 'MSMTNOTE1', 'MSMTNOTE2', 'MSMTNOTE3', 'DEATHRTE', 'FULL_NAME', 'CITIZEN_CODE', 'GENDER', 'MEMBER_ID']
    # dim_date: ['Date', 'Year', 'Quarter', 'Month', 'MonthName', 'DayOfYear', 'DayOfMonth', 'DayOfWeek', 'DayName', 'IsWeekend', 'Season', 'CreatedDate', 'ModifiedDate']
    # dim_nationality: ['CountryCode', 'CountryName', 'Region', 'SubRegion', 'IsActive', 'CreatedDate', 'ModifiedDate']
    # dim_peak: ['PeakID', 'PeakName', 'HeightMeters', 'ClimbingStatus', 'HostCountryCode', 'Coordinates', 'IsActive', 'CreatedDate', 'ModifiedDate']
    # dim_route: ['Route1', 'Route2', 'CombinedRoute', 'RouteType', 'DifficultyLevel', 'IsActive', 'CreatedDate', 'ModifiedDate']
    # dim_expedition_status: ['TerminationReason', 'StatusCategory', 'IsSuccess', 'StatusDescription', 'IsActive', 'CreatedDate', 'ModifiedDate']
    # dim_host_country: ['CountryCode', 'CountryName', 'Region', 'Subregion', 'CreatedDate', 'ModifiedDate', 'CountryKey']
    # dim_member: ['EXPID', 'MemberID', 'FullName', 'FNAME', 'LNAME', 'Gender', 'Age', 'AgeGroup', 'BirthYear', 'CitizenshipCountry', 'CreatedDate', 'ModifiedDate', 'MemberKey']    # And i have to get

    # ExpeditionKey ExpeditionID MemberKey DateKey PeakKey RouteKey StatusKey INT NOT HostCountryKey LeaderNationalityKey ExpeditionYear Season TotalMembers TotalDays Basecamp_Date Highpoint_Meters ExpeditionName Agency Sponsor IsSuccess SummitAttempts SummitSuccesses Deaths Injuries TotalCost MemberRole IsLeader IsDeputyLeader ReachedSummit Death Injury OxygenUsed AgeAtExpedition CreatedDate ModifiedDate 

    context.log.info("Head: \n" + str(dim_member.head()))    # Merge with members
    fact_df = cleaned_expeditions.merge(
        dim_member,
        left_on='EXPID',
        right_on='EXPID',
        how='right',
    )
    
    fact_df = fact_df.merge(
        cleaned_members,
        left_on='EXPID',
        right_on='EXPID',
        how='left',
    )
    
    # print out types of fact_df
    context.log.info(f"Fact DataFrame types:\n{fact_df.dtypes}")
    
    fact_expeditions = pd.DataFrame({
        # ExpeditionKey is auto-generated IDENTITY column - do not include
        'ExpeditionID': fact_df['EXPID'],
        'MemberKey': fact_df['MemberID'].fillna(0).astype(int),
        'DateKey': 0,  # Default value - would need lookup to DIM_Date
        'PeakKey': pd.to_numeric(fact_df['PEAKID_x'], errors='coerce').fillna(0).astype(int),
        'RouteKey': 0,  # Default value - would need lookup to DIM_Route
        'StatusKey': 0,  # Default value - would need lookup to DIM_ExpeditionStatus
        'HostCountryKey': 0,  # Default value - would need lookup to DIM_HostCountry
        'LeaderNationalityKey': 0,  # Default value - would need lookup to DIM_Nationality
        'ExpeditionYear': fact_df['YEAR'],
        'Season': fact_df['SEASON'],
        'TotalMembers': fact_df['TOTMEMBERS'].fillna(0),
        'TotalDays': fact_df['TOTDAYS'].fillna(0),
        'Basecamp_Date': fact_df['BCDATE'],
        'Highpoint_Meters': fact_df['HIGHPOINT'].fillna(0),
        'ExpeditionName': fact_df['EXPID'].astype(str),  # Using expedition ID as name
        'Agency': fact_df['AGENCY'],
        'Sponsor': fact_df['SPONSOR'],
        'IsSuccess': fact_df['IS_SUCCESS'].fillna(0).astype(int),
        'SummitAttempts': fact_df['TOTMEMBERS'].fillna(0),  # Total members as attempts
        'SummitSuccesses': fact_df['SMTMEMBERS'].fillna(0),
        'Deaths': fact_df['MDEATHS'].fillna(0) + fact_df['HDEATHS'].fillna(0),  # Member + hired deaths
        'Injuries': 0,  # Would need to calculate from injury data
        'TotalCost': None,  # Not available in source data
        'MemberRole': fact_df['STATUS'],
        'IsLeader': fact_df['LEADER'].fillna(0).astype(int),
        'IsDeputyLeader': fact_df['DEPUTY'].fillna(0).astype(int),
        'ReachedSummit': fact_df['MSUCCESS'].fillna(0).astype(int),
        'Death': fact_df['DEATH'].fillna(0).astype(int),
        'Injury': fact_df['INJURY'].fillna(0).astype(int),
        'OxygenUsed': fact_df['MO2USED'].fillna(0).astype(int),
        'AgeAtExpedition': fact_df['CALCAGE'].fillna(0),
        'CreatedDate': pd.Timestamp.now(),
        'ModifiedDate': pd.Timestamp.now()
    })

    # Clean and convert data types to prevent encoding and SQL errors
    fact_expeditions = fact_expeditions.copy()
    
    # Convert string columns to handle None/NaN values and encoding
    string_columns = ['ExpeditionID', 'Season', 'ExpeditionName', 'Agency', 'Sponsor', 'MemberRole']
    for col in string_columns:
        if col in fact_expeditions.columns:
            fact_expeditions[col] = fact_expeditions[col].astype(str).replace('nan', None)
    
    # Ensure integer columns are properly converted
    int_columns = ['MemberKey', 'DateKey', 'PeakKey', 'RouteKey', 'StatusKey', 'HostCountryKey', 
                   'LeaderNationalityKey', 'ExpeditionYear', 'TotalMembers', 'TotalDays', 
                   'Highpoint_Meters', 'IsSuccess', 'SummitAttempts', 'SummitSuccesses', 
                   'Deaths', 'Injuries', 'IsLeader', 'IsDeputyLeader', 'ReachedSummit', 
                   'Death', 'Injury', 'OxygenUsed', 'AgeAtExpedition']
    for col in int_columns:
        if col in fact_expeditions.columns:
            fact_expeditions[col] = pd.to_numeric(fact_expeditions[col], errors='coerce').fillna(0).astype(int)

    # show sample of fact_expeditions
    context.log.info(f"Sample of prepared fact expeditions data:\n{fact_expeditions.head()}")

    return fact_expeditions


@op(
    ins={
        "fact_data": In(pd.DataFrame),
        "dim_date_loaded": In(Dict[str, Any]),
        "dim_nationality_loaded": In(Dict[str, Any]),
        "dim_peak_loaded": In(Dict[str, Any]),
        "dim_route_loaded": In(Dict[str, Any]),
        "dim_expedition_status_loaded": In(Dict[str, Any]),
        "dim_host_country_loaded": In(Dict[str, Any]),
        "dim_member_loaded": In(Dict[str, Any]),
        "dim_country_indicators_loaded": In(Dict[str, Any]),
    },
    out=Out(Dict[str, Any], description="Load results"),
    config_schema=fact_config_schema,
    retry_policy=RetryPolicy(max_retries=3, delay=2),
    description="Load fact table data into the database",
    required_resource_keys={"db"}
)
def load_fact_expeditions(
    context,
    fact_data: pd.DataFrame,
    dim_date_loaded: Dict[str, Any],
    dim_nationality_loaded: Dict[str, Any],
    dim_peak_loaded: Dict[str, Any],
    dim_route_loaded: Dict[str, Any],
    dim_expedition_status_loaded: Dict[str, Any],   
    dim_host_country_loaded: Dict[str, Any],
    dim_member_loaded: Dict[str, Any],
    dim_country_indicators_loaded: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Load the prepared fact table data into the database.
    This operation depends on all dimension tables being loaded first.
    """
    
    # Log that all dependencies are satisfied
    context.log.info("All dimension tables loaded successfully:")
    context.log.info(f"  - Date: {dim_date_loaded.get('records_loaded', 0)} records")
    context.log.info(f"  - Nationality: {dim_nationality_loaded.get('records_loaded', 0)} records")
    context.log.info(f"  - Peak: {dim_peak_loaded.get('records_loaded', 0)} records")
    context.log.info(f"  - Route: {dim_route_loaded.get('records_loaded', 0)} records")
    context.log.info(f"  - Expedition Status: {dim_expedition_status_loaded.get('records_loaded', 0)} records")
    context.log.info(f"  - Host Country: {dim_host_country_loaded.get('records_loaded', 0)} records")
    context.log.info(f"  - Member: {dim_member_loaded.get('records_loaded', 0)} records")
    context.log.info(f"  - Country Indicators: {dim_country_indicators_loaded.get('records_loaded', 0)} records")
    
    context.log.info(f"Loading {len(fact_data)} fact records to database")
    
    try:
        # Load data using bulk insert
        load_result = context.resources.db.bulk_insert(
            df=fact_data,
            table_name='FACT_Expeditions',
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
    if 'ExpeditionYear' in fact_df.columns:
        valid_year_mask = (
            (fact_df['ExpeditionYear'] >= op_config["min_year"]) &
            (fact_df['ExpeditionYear'] <= op_config["max_year"])
        )
        fact_df = fact_df[valid_year_mask]
        
        removed_count = initial_count - len(fact_df)
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} records with invalid years")
    
    # Validate member counts are non-negative
    count_columns = ['TotalMembers', 'Deaths', 'SummitSuccesses', 'Injuries']
    for col in count_columns:
        if col in fact_df.columns:
            fact_df[col] = fact_df[col].clip(lower=0)
    
    # Validate boolean fields are 0 or 1
    boolean_columns = ['IsSuccess', 'IsLeader', 'IsDeputyLeader', 'ReachedSummit', 'Death', 'Injury', 'OxygenUsed']
    for col in boolean_columns:
        if col in fact_df.columns:
            fact_df[col] = fact_df[col].clip(0, 1)
    
    logger.info(f"Data validation completed. Final record count: {len(fact_df)}")
    
    return fact_df
