"""
Dimension processing operations for Himalayan Expeditions ETL pipeline.

This module contains Dagster ops for creating and loading dimension tables
in the data warehouse, including surrogate key generation and slowly
changing dimension handling.
"""

from typing import Dict, List, Any, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dagster import (
    In,
    op,
    Out,
    OpExecutionContext,
    RetryPolicy,
    Field,
    String,
    List as DagsterList,
    Array as DagsterArray,
)

from ..resources import DatabaseResource, ETLConfigResource, world_bank_config_schema


@op(
    name="create_dim_date",
    description="Create and populate the date dimension table",
    out=Out(pd.DataFrame, description="Date dimension DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"db", "etl_config"},
)
def create_dim_date(
    context: OpExecutionContext, cleaned_expeditions: pd.DataFrame
) -> pd.DataFrame:
    """
    Create the date dimension table covering the range of expedition dates.

    Generates a comprehensive date dimension with various date attributes
    useful for time-based analysis of expedition data.

    Args:
        cleaned_expeditions: Cleaned expeditions data to determine date range

    Returns:
        pd.DataFrame: Complete date dimension data
    """
    context.log.info("Creating date dimension")

    try:
        # Determine date range from expedition data
        min_year = 1900  # Default minimum
        max_year = datetime.now().year + 5  # Future years for planning

        if not cleaned_expeditions.empty and "YEAR" in cleaned_expeditions.columns:
            exp_min_year = cleaned_expeditions["YEAR"].min()
            exp_max_year = cleaned_expeditions["YEAR"].max()

            if pd.notna(exp_min_year):
                min_year = max(1900, int(exp_min_year) - 1)
            if pd.notna(exp_max_year):
                max_year = max(datetime.now().year + 5, int(exp_max_year) + 1)

        context.log.info(f"Creating date dimension for years {min_year} to {max_year}")

        # Generate date range
        start_date = datetime(min_year, 1, 1)
        end_date = datetime(max_year, 12, 31)

        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        # Create date dimension DataFrame
        dim_date = pd.DataFrame(
            {
                "Date": date_range,
                "Year": date_range.year,
                "Quarter": date_range.quarter,
                "Month": date_range.month,
                "MonthName": date_range.strftime("%B"),
                "DayOfYear": date_range.dayofyear,
                "DayOfMonth": date_range.day,
                "DayOfWeek": date_range.dayofweek + 1,  # 1=Monday, 7=Sunday
                "DayName": date_range.strftime("%A"),
                "IsWeekend": (date_range.dayofweek >= 5).astype(int),
            }
        )
        # Create DateKey and DATE_KEY columns for fact table joining
        dim_date["DATE_KEY"] = (
            dim_date["Year"].astype(str)
            + dim_date["Quarter"].astype(str).str.zfill(2)
            + "01"
        ).astype(int)
        dim_date["DateKey"] = range(1, len(dim_date) + 1)

        # Add season based on month
        def get_season(month):
            if month in [12, 1, 2]:
                return "Winter"
            elif month in [3, 4, 5]:
                return "Spring"
            elif month in [6, 7, 8]:
                return "Summer"
            else:
                return "Autumn"

        dim_date["Season"] = dim_date["Month"].apply(get_season)

        # Add metadata
        dim_date["CreatedDate"] = datetime.now()
        dim_date["ModifiedDate"] = datetime.now()

        context.log.info(f"Created date dimension with {len(dim_date)} records")
        context.log.info(
            f"Date range: {dim_date['Date'].min()} to {dim_date['Date'].max()}"
        )

        return dim_date

    except Exception as e:
        context.log.error(f"Failed to create date dimension: {str(e)}")
        raise


@op(
    name="create_dim_nationality",
    description="Create and populate the nationality dimension table",
    out=Out(pd.DataFrame, description="Nationality dimension DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"db", "etl_config"},
)
def create_dim_nationality(
    context: OpExecutionContext,
    cleaned_expeditions: pd.DataFrame,
    cleaned_members: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create the nationality dimension table from expedition and member data.

    Extracts unique nationalities/countries from both expedition leaders
    and individual members, standardizing country codes and names.

    Args:
        cleaned_expeditions: Cleaned expeditions data
        cleaned_members: Cleaned members data

    Returns:
        pd.DataFrame: Nationality dimension data
    """
    config: ETLConfigResource = context.resources.etl_config

    context.log.info("Creating nationality dimension")

    try:
        nationalities = set()

        # Extract nationalities from expeditions (leader countries)
        if not cleaned_expeditions.empty:
            exp_cols = ["NATION", "HOST", "COUNTRY"]
            for col in exp_cols:
                if col in cleaned_expeditions.columns:
                    exp_nations = (
                        cleaned_expeditions[col]
                        .dropna()
                        .astype(str)
                        .str.upper()
                        .str.strip()
                    )
                    nationalities.update(exp_nations[exp_nations != "NAN"])

        # Extract nationalities from members
        if not cleaned_members.empty:
            member_cols = ["CITIZEN", "CITIZEN_CODE", "NATION"]
            for col in member_cols:
                if col in cleaned_members.columns:
                    member_nations = (
                        cleaned_members[col]
                        .dropna()
                        .astype(str)
                        .str.upper()
                        .str.strip()
                    )
                    nationalities.update(member_nations[member_nations != "NAN"])

        # Remove empty strings and invalid entries
        nationalities = {
            n for n in nationalities if n and n not in ["", "NAN", "UNKNOWN", "NULL"]
        }

        context.log.info(f"Found {len(nationalities)} unique nationalities")

        # Create nationality dimension
        nationality_records = []

        for nationality in sorted(nationalities):
            # Try to get ISO3 code from mapping
            country_code = config.country_code_mapping.get(nationality, nationality[:3])

            # Get standardized country name
            country_name = _get_standardized_country_name(nationality, config)

            # Get region information (simplified)
            region, subregion = _get_country_region(country_code)
            #
            nationality_records.append(
                {
                    "CountryCode": country_code,
                    "CountryName": country_name,
                    "Region": region,
                    "SubRegion": subregion,
                    "IsActive": True,
                    "CreatedDate": datetime.now(),
                    "ModifiedDate": datetime.now(),
                }
            )

        # Add unknown/default entry
        nationality_records.append(
            {
                "CountryCode": "UNK",
                "CountryName": "Unknown",
                "Region": "Unknown",
                "SubRegion": "Unknown",
                "IsActive": True,
                "CreatedDate": datetime.now(),
                "ModifiedDate": datetime.now(),
            }
        )
        dim_nationality = pd.DataFrame(nationality_records)

        # Remove duplicates based on country code
        dim_nationality = dim_nationality.drop_duplicates(
            subset=["CountryCode"], keep="first"
        )

        # Add surrogate key
        dim_nationality["NationalityKey"] = range(1, len(dim_nationality) + 1)

        context.log.info(
            f"Created nationality dimension with {len(dim_nationality)} records"
        )

        return dim_nationality

    except Exception as e:
        context.log.error(f"Failed to create nationality dimension: {str(e)}")
        raise


@op(
    name="create_dim_peak",
    description="Create and populate the peak dimension table",
    out=Out(pd.DataFrame, description="Peak dimension DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"db", "etl_config"},
)
def create_dim_peak(
    context: OpExecutionContext,
    cleaned_peaks: pd.DataFrame,
    cleaned_expeditions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create the peak dimension table from peaks and expedition data.

    Combines peak master data with expedition-derived information
    to create a comprehensive peak dimension.

    Args:
        cleaned_peaks: Cleaned peaks master data
        cleaned_expeditions: Cleaned expeditions data for additional peak info

    Returns:
        pd.DataFrame: Peak dimension data
    """
    context.log.info("Creating peak dimension")

    try:
        if cleaned_peaks.empty:
            context.log.warning("No peaks data available")
            return pd.DataFrame()

        # Start with cleaned peaks data
        dim_peak = cleaned_peaks.copy()

        # Standardize column names for dimension
        column_mapping = {
            "PEAKID": "PeakID",
            "PKNAME": "PeakName",
            "PKNAME_CLEANED": "PeakName",
            "HEIGHTM": "HeightMeters",
            "CLIMBING_STATUS": "ClimbingStatus",
            "PSTATUS": "ClimbingStatus",
            "FIRST_ASCENT_YEAR": "FirstAscentYear",
            "YRDSC": "FirstAscentYear",
            "LOCATION": "Location",
            "REGION": "Region",
        }

        # Rename columns if they exist
        for old_col, new_col in column_mapping.items():
            if old_col in dim_peak.columns and new_col not in dim_peak.columns:
                dim_peak = dim_peak.rename(columns={old_col: new_col})

        # Ensure required columns exist
        required_columns = ["PeakID", "PeakName", "HeightMeters"]
        for col in required_columns:
            if col not in dim_peak.columns:
                if col == "PeakID":
                    dim_peak[col] = dim_peak.index.astype(str)
                elif col == "PeakName":
                    dim_peak[col] = "Unknown Peak"
                elif col == "HeightMeters":
                    dim_peak[col] = np.nan

        # Add expedition-derived information
        if not cleaned_expeditions.empty and "PEAKID" in cleaned_expeditions.columns:
            # Get host country information from expeditions
            peak_countries = (
                cleaned_expeditions.groupby("PEAKID")["HOST"].first().reset_index()
            )
            peak_countries.columns = ["PeakID", "HostCountryCode"]

            # Merge with dimension
            dim_peak = dim_peak.merge(peak_countries, on="PeakID", how="left")

        # Clean and standardize values
        if "PeakName" in dim_peak.columns:
            dim_peak["PeakName"] = dim_peak["PeakName"].fillna("Unknown Peak")

        if "ClimbingStatus" in dim_peak.columns:
            dim_peak["ClimbingStatus"] = dim_peak["ClimbingStatus"].fillna("Unknown")

        if "HostCountryCode" in dim_peak.columns:
            dim_peak["HostCountryCode"] = dim_peak["HostCountryCode"].fillna("UNK")

        # Add coordinates if available
        if "LAT" in dim_peak.columns and "LON" in dim_peak.columns:
            dim_peak["Coordinates"] = dim_peak.apply(
                lambda row: (
                    f"{row['LAT']:.6f},{row['LON']:.6f}"
                    if pd.notna(row["LAT"]) and pd.notna(row["LON"])
                    else None
                ),
                axis=1,
            )
        else:
            dim_peak["Coordinates"] = None

        # Add metadata
        dim_peak["IsActive"] = True
        dim_peak["CreatedDate"] = datetime.now()
        dim_peak["ModifiedDate"] = datetime.now()

        # Select final columns for dimension
        final_columns = [
            "PeakID",
            "PeakName",
            "HeightMeters",
            "ClimbingStatus",
            "FirstAscentYear",
            "HostCountryCode",
            "Coordinates",
            "IsActive",
            "CreatedDate",
            "ModifiedDate",
        ]

        # Only include columns that exist
        available_columns = [col for col in final_columns if col in dim_peak.columns]
        dim_peak = dim_peak[available_columns]
        # Remove duplicates
        dim_peak = dim_peak.drop_duplicates(subset=["PeakID"], keep="first")

        # Add surrogate key
        dim_peak["PeakKey"] = range(1, len(dim_peak) + 1)

        context.log.info(f"Created peak dimension with {len(dim_peak)} records")
        context.log.info(
            f"Height range: {dim_peak['HeightMeters'].min():.0f}m to {dim_peak['HeightMeters'].max():.0f}m"
        )

        return dim_peak

    except Exception as e:
        context.log.error(f"Failed to create peak dimension: {str(e)}")
        raise


@op(
    name="create_dim_expedition_status",
    description="Create and populate the expedition status dimension table",
    out=Out(pd.DataFrame, description="Expedition status dimension DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"db", "etl_config"},
)
def create_dim_expedition_status(
    context: OpExecutionContext, cleaned_expeditions: pd.DataFrame
) -> pd.DataFrame:
    """
    Create the expedition status dimension table from termination reasons.

    Categorizes and standardizes expedition termination reasons
    into a structured dimension for analysis.

    Args:
        cleaned_expeditions: Cleaned expeditions data

    Returns:
        pd.DataFrame: Expedition status dimension data
    """
    config: ETLConfigResource = context.resources.etl_config

    context.log.info("Creating expedition status dimension")

    try:
        # Get unique termination reasons from expeditions
        if cleaned_expeditions.empty or "TERMREASON" not in cleaned_expeditions.columns:
            context.log.warning("No termination reason data available")
            term_reasons = set()
        else:
            term_reasons = set(
                cleaned_expeditions["TERMREASON"]
                .dropna()
                .astype(str)
                .str.upper()
                .str.strip()
            )
            term_reasons = {r for r in term_reasons if r and r not in ["", "NAN"]}

        # Use predefined mapping and add any new reasons found
        all_reasons = set(config.termination_reason_mapping.keys()) | term_reasons

        status_records = []

        for reason in sorted(all_reasons):
            # Get mapping information
            mapping_info = config.termination_reason_mapping.get(
                reason, {"category": "Other", "is_success": False}
            )

            status_records.append(
                {
                    "TerminationReason": reason,
                    "StatusCategory": mapping_info["category"],
                    "IsSuccess": mapping_info["is_success"],
                    "StatusDescription": _get_status_description(reason),
                    "IsActive": True,
                    "CreatedDate": datetime.now(),
                    "ModifiedDate": datetime.now(),
                }
            )

        # Add unknown status
        status_records.append(
            {
                "TerminationReason": "UNKNOWN",
                "StatusCategory": "Unknown",
                "IsSuccess": False,
                "StatusDescription": "Unknown expedition outcome",
                "IsActive": True,
                "CreatedDate": datetime.now(),
                "ModifiedDate": datetime.now(),
            }
        )
        dim_status = pd.DataFrame(status_records)

        # Remove duplicates
        dim_status = dim_status.drop_duplicates(
            subset=["TerminationReason"], keep="first"
        )

        # Add surrogate key
        dim_status["StatusKey"] = range(1, len(dim_status) + 1)

        context.log.info(
            f"Created expedition status dimension with {len(dim_status)} records"
        )

        context.log.info(
            f"Created expedition status dimension with {len(dim_status)} records"
        )

        # Log distribution of status categories
        category_counts = dim_status["StatusCategory"].value_counts()
        context.log.info(f"Status categories: {category_counts.to_dict()}")

        return dim_status

    except Exception as e:
        context.log.error(f"Failed to create expedition status dimension: {str(e)}")
        raise


@op(
    name="create_dim_route",
    description="Create and populate the route dimension table",
    out=Out(pd.DataFrame, description="Route dimension DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"db", "etl_config"},
)
def create_dim_route(
    context: OpExecutionContext, cleaned_expeditions: pd.DataFrame
) -> pd.DataFrame:
    """
    Create the route dimension table from expedition route information.

    Extracts and standardizes climbing route information from expeditions
    to create a route dimension for analysis.

    Args:
        cleaned_expeditions: Cleaned expeditions data

    Returns:
        pd.DataFrame: Route dimension data
    """
    context.log.info("Creating route dimension")

    try:
        if cleaned_expeditions.empty:
            context.log.warning("No expedition data available for routes")
            return pd.DataFrame()

        # Extract route combinations
        route_data = []

        for _, row in cleaned_expeditions.iterrows():
            route1 = row.get("ROUTE1", "") if pd.notna(row.get("ROUTE1")) else ""
            route2 = row.get("ROUTE2", "") if pd.notna(row.get("ROUTE2")) else ""

            # Create combined route
            route_parts = [r.strip() for r in [route1, route2] if r.strip()]
            combined_route = " / ".join(route_parts) if route_parts else "Unknown Route"

            # Determine route type
            route_type = _determine_route_type(route1, route2)

            # Estimate difficulty (simplified)
            difficulty = _estimate_route_difficulty(route1, route2)

            route_data.append(
                {
                    "Route1": route1 if route1 else None,
                    "Route2": route2 if route2 else None,
                    "CombinedRoute": combined_route,
                    "RouteType": route_type,
                    "DifficultyLevel": difficulty,
                }
            )

        # Create DataFrame and remove duplicates
        dim_route = pd.DataFrame(route_data)
        dim_route = dim_route.drop_duplicates(subset=["Route1", "Route2"], keep="first")

        # Add metadata
        dim_route["IsActive"] = True
        dim_route["CreatedDate"] = datetime.now()
        dim_route["ModifiedDate"] = datetime.now()

        # Add unknown route
        unknown_route = pd.DataFrame(
            [
                {
                    "Route1": None,
                    "Route2": None,
                    "CombinedRoute": "Unknown Route",
                    "RouteType": "Unknown",
                    "DifficultyLevel": "Unknown",
                    "IsActive": True,
                    "CreatedDate": datetime.now(),
                    "ModifiedDate": datetime.now(),
                }
            ]
        )
        dim_route = pd.concat([dim_route, unknown_route], ignore_index=True)

        # Add surrogate key
        dim_route["RouteKey"] = range(1, len(dim_route) + 1)

        context.log.info(f"Created route dimension with {len(dim_route)} records")

        # Log route type distribution
        type_counts = dim_route["RouteType"].value_counts()
        context.log.info(f"Route types: {type_counts.to_dict()}")

        return dim_route

    except Exception as e:
        context.log.error(f"Failed to create route dimension: {str(e)}")
        raise


# Helper functions for dimension processing


def _get_standardized_country_name(nationality: str, config: ETLConfigResource) -> str:
    """Get standardized country name from nationality code or name."""
    # Check if it's already a country name in the mapping
    for country_name, code in config.country_code_mapping.items():
        if nationality.upper() == code or nationality.upper() == country_name.upper():
            return country_name.title()

    # Return cleaned version of the input
    return nationality.title().replace("_", " ")


def _get_country_region(country_code: str) -> Tuple[str, str]:
    """Get region and subregion for a country (simplified mapping)."""

    # Simplified regional mapping
    region_mapping = {
        # South Asia
        "NPL": ("Asia", "South Asia"),
        "IND": ("Asia", "South Asia"),
        "PAK": ("Asia", "South Asia"),
        "BGD": ("Asia", "South Asia"),
        "BTN": ("Asia", "South Asia"),
        "LKA": ("Asia", "South Asia"),
        # East Asia
        "CHN": ("Asia", "East Asia"),
        "JPN": ("Asia", "East Asia"),
        "KOR": ("Asia", "East Asia"),
        "PRK": ("Asia", "East Asia"),
        "MNG": ("Asia", "East Asia"),
        # Southeast Asia
        "MMR": ("Asia", "Southeast Asia"),
        "THA": ("Asia", "Southeast Asia"),
        "VNM": ("Asia", "Southeast Asia"),
        "LAO": ("Asia", "Southeast Asia"),
        # Europe
        "GBR": ("Europe", "Western Europe"),
        "FRA": ("Europe", "Western Europe"),
        "DEU": ("Europe", "Western Europe"),
        "ITA": ("Europe", "Western Europe"),
        "RUS": ("Europe", "Eastern Europe"),
        # North America
        "USA": ("Americas", "North America"),
        "CAN": ("Americas", "North America"),
        "MEX": ("Americas", "North America"),
        # Others
        "AUS": ("Oceania", "Australia and New Zealand"),
        "NZL": ("Oceania", "Australia and New Zealand"),
    }

    return region_mapping.get(country_code, ("Unknown", "Unknown"))


def _get_status_description(reason: str) -> str:
    """Get human-readable description for termination reason."""
    descriptions = {
        "SUCCESS (MAIN PEAK)": "Successfully reached the main peak summit",
        "SUCCESS (SUBPEAK)": "Successfully reached a subsidiary peak",
        "UNSUCCESSFUL (ATTEMPT)": "Unsuccessful summit attempt",
        "UNSUCCESSFUL (OTHER)": "Unsuccessful expedition due to other reasons",
        "ABANDONED": "Expedition abandoned before completion",
        "ACCIDENT": "Expedition terminated due to accident",
        "ILLNESS": "Expedition terminated due to illness",
        "WEATHER": "Expedition terminated due to weather conditions",
        "ROUTE": "Expedition terminated due to route conditions",
        "PERMITS": "Expedition terminated due to permit issues",
        "UNKNOWN": "Unknown expedition outcome",
    }

    return descriptions.get(reason, f"Expedition outcome: {reason.lower()}")


def _determine_route_type(route1: str, route2: str) -> str:
    """Determine route type based on route names."""
    combined = f"{route1} {route2}".upper()

    if any(word in combined for word in ["NORTH", "N FACE", "NORTH FACE"]):
        return "North Face"
    elif any(word in combined for word in ["SOUTH", "S FACE", "SOUTH FACE"]):
        return "South Face"
    elif any(word in combined for word in ["EAST", "E FACE", "EAST FACE"]):
        return "East Face"
    elif any(word in combined for word in ["WEST", "W FACE", "WEST FACE"]):
        return "West Face"
    elif any(word in combined for word in ["RIDGE", "ARETE"]):
        return "Ridge"
    elif any(word in combined for word in ["COULOIR", "GULLY"]):
        return "Couloir"
    elif any(word in combined for word in ["GLACIER"]):
        return "Glacier"
    else:
        return "Standard"


def _estimate_route_difficulty(route1: str, route2: str) -> str:
    """Estimate route difficulty based on route description."""
    combined = f"{route1} {route2}".upper()

    # Technical routes
    if any(word in combined for word in ["DIRECT", "TECHNICAL", "STEEP"]):
        return "Technical"
    # Normal routes
    elif any(word in combined for word in ["NORMAL", "STANDARD", "REGULAR"]):
        return "Normal"
    # Easy routes
    elif any(word in combined for word in ["EASY", "SIMPLE"]):
        return "Easy"
    else:
        return "Unknown"


@op(
    name="create_dim_host_country",
    description="Create host country dimension from expedition data",
    out=Out(pd.DataFrame, description="Host country dimension DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"db", "etl_config"},
)
def create_dim_host_country(
    context: OpExecutionContext, enhanced_expeditions: pd.DataFrame
) -> pd.DataFrame:
    """
    Create the host country dimension table from expedition data.

    Args:
        enhanced_expeditions: Cleaned expeditions data

    Returns:
        pd.DataFrame: Host country dimension data
    """
    context.log.info("Creating host country dimension")
    config: ETLConfigResource = context.resources.etl_config

    try:
        # Extract unique host countries from expeditions using HOST column
        if "HOST" not in enhanced_expeditions.columns:
            context.log.error("HOST column not found in expeditions data")
            raise ValueError("HOST column not found in expeditions data")

        # Get unique host country codes
        host_countries = enhanced_expeditions["HOST"].dropna().unique()
        context.log.info(
            f"Found {len(host_countries)} unique host countries: {sorted(host_countries)}"
        )

        # Create country mapping from HOST codes to names
        country_mapping = {
            "1": "Nepal",
            "2": "India",
            "3": "Pakistan",
            "4": "China",
            "5": "Tibet",  # Historical reference
            "6": "Bhutan",
            "7": "Myanmar",
            "8": "Japan",  # Some peaks may be listed with different host codes
        }

        # Create dimension records
        host_country_records = []
        for host_code in sorted(host_countries):
            host_str = str(int(host_code)) if pd.notna(host_code) else "UNK"
            country_name = country_mapping.get(host_str, f"Country_{host_str}")
            standardized_name = _get_standardized_country_name(country_name, config)
            host_country_records.append(
                {
                    "CountryCode": host_str,
                    "CountryName": standardized_name,
                    "CreatedDate": datetime.now(),
                    "ModifiedDate": datetime.now(),
                }
            )

        # Add unknown country entry
        host_country_records.append(
            {
                "CountryCode": "UNK",
                "CountryName": "Unknown",
                "Region": "Unknown",
                "Subregion": "Unknown",
                "CreatedDate": datetime.now(),
                "ModifiedDate": datetime.now(),
            }
        )

        # Create dimension DataFrame
        dim_host_country = pd.DataFrame(host_country_records)

        # Remove duplicates and sort
        dim_host_country = dim_host_country.drop_duplicates(subset=["CountryCode"])
        dim_host_country = dim_host_country.sort_values("CountryCode").reset_index(
            drop=True
        )
        # Add surrogate key
        dim_host_country["HostCountryKey"] = range(1, len(dim_host_country) + 1)

        context.log.info(
            f"Created host country dimension with {len(dim_host_country)} records"
        )
        return dim_host_country

    except Exception as e:
        context.log.error(f"Error creating host country dimension: {str(e)}")
        raise


@op(
    name="create_dim_member",
    description="Create member dimension from members data",
    out=Out(pd.DataFrame, description="Member dimension DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"db", "etl_config"},
)
def create_dim_member(
    context: OpExecutionContext, enhanced_members: pd.DataFrame
) -> pd.DataFrame:
    """
    Create the member dimension table from members data.

    Args:
        enhanced_members: Cleaned members data

    Returns:
        pd.DataFrame: Member dimension data
    """
    context.log.info("Creating member dimension")

    context.log.info(f"Head of enhanced members data:\n{enhanced_members.head()}")
    try:
        # Create member dimension with key attributes including MEMBID for unique identification
        required_columns = [
            "EXPID",
            "MEMBID",
            "FNAME",
            "LNAME",
            "MYEAR",
            "SEX",
            "AGE",
            "CITIZEN",
            "CALCAGE",
        ]
        available_columns = [
            col for col in required_columns if col in enhanced_members.columns
        ]

        context.log.info(f"Available columns for member dimension: {available_columns}")

        dim_member = enhanced_members[available_columns].copy()

        # Add full name
        dim_member["FullName"] = (
            dim_member["FNAME"].fillna("") + " " + dim_member["LNAME"].fillna("")
        ).str.strip()

        # Clean and standardize data
        dim_member["Gender"] = (
            dim_member["SEX"].map({"M": "Male", "F": "Female"}).fillna("Unknown")
        )
        dim_member["CitizenshipCountry"] = dim_member["CITIZEN"].fillna("Unknown")
        dim_member["Age"] = dim_member["CALCAGE"].fillna(-1).astype(int)
        dim_member["BirthYear"] = dim_member["MYEAR"].fillna(-1).astype(int)

        # Keep the original member ID for bridge table linkage
        dim_member["MemberID"] = dim_member["MEMBID"].fillna("Unknown")

        # Add age groups
        def get_age_group(age):
            if age < 0:
                return "Unknown"
            elif age < 18:
                return "Under 18"
            elif age < 30:
                return "18-29"
            elif age < 40:
                return "30-39"
            elif age < 50:
                return "40-49"
            elif age < 60:
                return "50-59"
            else:
                return "60+"

        dim_member["AgeGroup"] = dim_member["Age"].apply(get_age_group)

        # Add metadata
        dim_member["CreatedDate"] = datetime.now()
        dim_member["ModifiedDate"] = datetime.now()
        dim_member = dim_member[
            [
                "EXPID",
                "MemberID",
                "FullName",
                "FNAME",
                "LNAME",
                "Gender",
                "Age",
                "AgeGroup",
                "BirthYear",
                "CitizenshipCountry",
                "CreatedDate",
                "ModifiedDate",
            ]
        ]

        # Remove duplicates based on key attributes
        dim_member = dim_member.drop_duplicates(subset=["MemberID"], keep="first")
        dim_member = dim_member.reset_index(drop=True)

        # Add surrogate key
        dim_member["MemberKey"] = range(1, len(dim_member) + 1)

        context.log.info(f"Created member dimension with {len(dim_member)} records")

        context.log.info(f"Member dimension head:\n{dim_member.head()}")
        return dim_member

    except Exception as e:
        context.log.error(f"Error creating member dimension: {str(e)}")
        raise


@op(
    ins={"cleaned_wb_data": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Country indicators dimension"),
    config_schema=world_bank_config_schema,
    retry_policy=RetryPolicy(max_retries=2, delay=2),
    description="Create country indicators dimension table",
)
def create_dim_country_indicators(
    context, cleaned_wb_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Create the DIM_CountryIndicators dimension table.

    This dimension contains economic and social indicators for countries
    organized by country and year to support trend analysis.
    """
    context.log.info(
        f"Creating country indicators dimension from {len(cleaned_wb_data)} records"
    )

    # Debug: show cleaned_wb_data structure
    context.log.info(f"Cleaned WB data columns: {list(cleaned_wb_data.columns)}")
    if not cleaned_wb_data.empty:
        context.log.info(f"Sample cleaned data:\n{cleaned_wb_data.head()}")

    try:
        # Pivot the data to have indicators as columns
        # Use the correct column names from the cleaned_wb_data
        pivot_df = cleaned_wb_data.pivot_table(
            index=["Country Code", "Country Name", "Year"],
            columns="Series Code",
            values="Value",
            aggfunc="mean",  # In case of duplicates
        ).reset_index()

        # Flatten column names
        pivot_df.columns.name = None
        # Rename indicator columns to more readable names
        indicator_mappings = {
            "NY.GDP.PCAP.CD": "GDP_PER_CAPITA_USD",
            "HD.HCI.OVRL": "HUMAN_CAPITAL_INDEX",
            "IT.NET.USER.ZS": "INTERNET_USERS_PERCENT",
            "SH.MED.PHYS.ZS": "PHYSICIANS_PER_1000",
            "PV.EST": "POLITICAL_STABILITY_INDEX",
        }

        # Rename columns
        pivot_df = pivot_df.rename(columns=indicator_mappings)

        # Create dimension key
        pivot_df["INDICATOR_KEY"] = range(1, len(pivot_df) + 1)
        # Rename standard columns
        pivot_df = pivot_df.rename(
            columns={
                "Country Code": "COUNTRY_CODE",
                "Country Name": "COUNTRY_NAME",
                "Year": "YEAR",
            }
        )
        # Calculate derived indicators (removed GDP_TOTAL calculation since no population data)
        # Could add other derived metrics here if needed

        # Add metadata columns
        pivot_df["LAST_UPDATED"] = datetime.now()
        pivot_df["DATA_SOURCE"] = "World Bank API"
        # Select final columns
        dimension_columns = [
            "INDICATOR_KEY",
            "COUNTRY_CODE",
            "COUNTRY_NAME",
            "YEAR",
            "GDP_PER_CAPITA_USD",
            "HUMAN_CAPITAL_INDEX",
            "INTERNET_USERS_PERCENT",
            "PHYSICIANS_PER_1000",
            "POLITICAL_STABILITY_INDEX",
            "LAST_UPDATED",
            "DATA_SOURCE",
        ]

        # Keep only columns that exist
        available_columns = [
            col for col in dimension_columns if col in pivot_df.columns
        ]
        dim_indicators = pivot_df[available_columns]

        # Sort by country and year
        dim_indicators = dim_indicators.sort_values(["COUNTRY_NAME", "YEAR"])

        context.log.info(
            f"Created country indicators dimension with {len(dim_indicators)} records"
        )

        return dim_indicators

    except Exception as e:
        context.log.error(f"Error creating country indicators dimension: {str(e)}")
        raise
