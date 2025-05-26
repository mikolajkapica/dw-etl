import re
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from thefuzz import fuzz, process
from dagster import (
    op,
    Out,
    OpExecutionContext,
    RetryPolicy,
)

from ..resources import ETLConfigResource


@op(
    name="clean_expeditions_data",
    description="Clean and standardize expeditions data",
    out=Out(pd.DataFrame, description="Cleaned expeditions DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"etl_config"},
)
def clean_expeditions_data(
    context: OpExecutionContext, raw_expeditions: pd.DataFrame
) -> pd.DataFrame:
    """
    Clean and standardize expeditions data.

    Performs the following cleaning operations:
    - Handle duplicate EXPIDs with deduplication strategy
    - Convert date fields to proper datetime format
    - Standardize season codes using mapping
    - Convert numeric fields to appropriate data types
    - Parse text fields like LEADERS into structured format
    - Handle missing values with appropriate strategies

    Args:
        raw_expeditions: Raw expeditions DataFrame from extraction

    Returns:
        pd.DataFrame: Cleaned expeditions data ready for transformation
    """
    config: ETLConfigResource = context.resources.etl_config

    context.log.info("Starting expeditions data cleaning")

    if raw_expeditions.empty:
        context.log.warning("Empty expeditions DataFrame received")
        return raw_expeditions

    df = raw_expeditions.copy()
    initial_rows = len(df)

    try:
        # 1. Handle duplicate EXPIDs
        duplicate_count = df["EXPID"].duplicated().sum()
        if duplicate_count > 0:
            context.log.warning(f"Found {duplicate_count} duplicate EXPID records")

            # Strategy: Keep the most recent record based on available data completeness
            df["_completeness_score"] = df.notna().sum(axis=1)
            df = df.sort_values(
                ["EXPID", "_completeness_score"], ascending=[True, False]
            )
            df = df.drop_duplicates(subset=["EXPID"], keep="first")
            df = df.drop("_completeness_score", axis=1)

            context.log.info(f"Removed {duplicate_count} duplicate records")

        # 2. Clean and convert date fields
        date_columns = ["SMTDATE", "BCDATE", "STDATE"]
        for col in date_columns:
            if col in df.columns:
                df[col] = _clean_date_column(df[col], context, col)

        # 3. Standardize SEASON codes
        if "SEASON" in df.columns:
            df["SEASON"] = df["SEASON"].astype(str).str.upper().str.strip()
            df["SEASON"] = df["SEASON"].map(config.season_mapping).fillna(df["SEASON"])

            # Log season distribution
            season_counts = df["SEASON"].value_counts()
            context.log.info(f"Season distribution: {season_counts.to_dict()}")

        # 4. Convert numeric fields
        numeric_columns = {
            "YEAR": "int",
            "HEIGHTM": "float",
            "TOTMEMBERS": "int",
            "SMTMEMBERS": "int",
            "TOTDAYS": "int",
            "MDEATHS": "int",
            "HDEATHS": "int",
        }

        for col, dtype in numeric_columns.items():
            if col in df.columns:
                df[col] = _convert_to_numeric(df[col], dtype, context, col)

        # 5. Standardize termination reason
        if "TERMREASON" in df.columns:
            df["TERMREASON"] = df["TERMREASON"].astype(str).str.upper().str.strip()
            df["TERMREASON"] = df["TERMREASON"].replace("NAN", "UNKNOWN")

        # 6. Parse LEADERS field into structured format
        if "LEADERS" in df.columns:
            df["LEADERS_LIST"] = df["LEADERS"].apply(_parse_leaders_field)
            df["LEADER_COUNT"] = df["LEADERS_LIST"].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )

        # 7. Clean text fields
        text_columns = ["PKNAME", "EXPNAME", "ROUTE1", "ROUTE2", "AGENCY", "SPONSOR"]
        for col in text_columns:
            if col in df.columns:
                df[col] = _clean_text_field(df[col])

        # 8. Create derived fields
        df["IS_SUCCESS"] = _determine_expedition_success(df)
        df["TOTAL_DEATHS"] = df.get("MDEATHS", 0).fillna(0) + df.get(
            "HDEATHS", 0
        ).fillna(0)

        # 9. Handle remaining missing values
        df = _handle_missing_values(df, context)

        # 10. Validate data quality after cleaning
        _validate_cleaned_data(df, context)

        final_rows = len(df)
        context.log.info(f"Expeditions cleaning completed")
        context.log.info(
            f"Rows: {initial_rows} → {final_rows} ({initial_rows - final_rows} removed)"
        )

        return df

    except Exception as e:
        context.log.error(f"Failed to clean expeditions data: {str(e)}")
        raise


@op(
    name="clean_members_data",
    description="Clean and standardize members data",
    out=Out(pd.DataFrame, description="Cleaned members DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"etl_config"},
)
def clean_members_data(
    context: OpExecutionContext, raw_members: pd.DataFrame
) -> pd.DataFrame:
    """
    Clean and standardize expedition members data.

    Performs the following cleaning operations:
    - Standardize member names (first name, last name)
    - Handle missing member information
    - Convert age and other numeric fields
    - Standardize citizenship/nationality codes
    - Create unique member identifiers
    - Handle gender classification

    Args:
        raw_members: Raw members DataFrame from extraction

    Returns:
        pd.DataFrame: Cleaned members data ready for transformation
    """
    config: ETLConfigResource = context.resources.etl_config

    context.log.info("Starting members data cleaning")

    # sample head
    context.log.info(f"Raw members data sample:\n{raw_members.head()}")

    if raw_members.empty:
        context.log.warning("Empty members DataFrame received")
        return raw_members

    df = raw_members.copy()
    initial_rows = len(df)

    try:
        # 1. Clean and standardize names
        if "FNAME" in df.columns:
            df["FNAME"] = _clean_name_field(df["FNAME"])
        if "LNAME" in df.columns:
            df["LNAME"] = _clean_name_field(df["LNAME"])

        # 2. Create full name field
        df["FULL_NAME"] = _create_full_name(df.get("FNAME", ""), df.get("LNAME", ""))

        # 3. Clean citizenship/nationality
        if "CITIZEN" in df.columns:
            df["CITIZEN"] = df["CITIZEN"].astype(str).str.upper().str.strip()
            df["CITIZEN"] = df["CITIZEN"].replace("NAN", "UNKNOWN")
            # Map country names to ISO codes
            df["CITIZEN_CODE"] = (
                df["CITIZEN"].map(config.country_code_mapping).fillna(df["CITIZEN"])
            )

        # 4. Convert numeric fields
        numeric_columns = {
            "AGE": "int",
            "CALCAGE": "int",
            "MYEAR": "int",
        }

        for col, dtype in numeric_columns.items():
            if col in df.columns:
                df[col] = _convert_to_numeric(df[col], dtype, context, col)

        # 5. Standardize gender
        if "SEX" in df.columns:
            df["SEX"] = df["SEX"].astype(str).str.upper().str.strip()
            gender_mapping = {
                "M": "Male",
                "F": "Female",
                "MALE": "Male",
                "FEMALE": "Female",
            }
            df["GENDER"] = df["SEX"].map(gender_mapping).fillna("Unknown")

        # 6. Handle status fields
        status_fields = ["STATUS", "DEATH", "DEATHTYPE", "DEATHHEIGHTM"]
        for col in status_fields:
            if col in df.columns:
                if col in ["DEATH"]:
                    # Convert to boolean
                    df[col] = (
                        df[col].astype(str).str.upper().isin(["TRUE", "1", "Y", "YES"])
                    )
                elif col == "DEATHHEIGHTM":
                    df[col] = _convert_to_numeric(df[col], "float", context, col)
                else:
                    df[col] = _clean_text_field(df[col])

        # 7. Create member identifier
        df["MEMBER_ID"] = _create_member_identifier(df)

        # 8. Handle missing values
        df = _handle_missing_values(df, context)

        # 9. Remove rows with insufficient data
        # Keep members with at least name information or member ID
        valid_members = (
            df["FULL_NAME"].notna() & (df["FULL_NAME"].str.strip() != "")
            | df["MEMBER_ID"].notna()
        )

        invalid_count = (~valid_members).sum()
        if invalid_count > 0:
            context.log.warning(
                f"Removing {invalid_count} members with insufficient data"
            )
            df = df[valid_members]

        final_rows = len(df)
        context.log.info(f"Members cleaning completed")
        context.log.info(
            f"Rows: {initial_rows} → {final_rows} ({initial_rows - final_rows} removed)"
        )

        return df

    except Exception as e:
        context.log.error(f"Failed to clean members data: {str(e)}")
        raise


@op(
    name="clean_peaks_data",
    description="Clean and standardize peaks data",
    out=Out(pd.DataFrame, description="Cleaned peaks DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"etl_config"},
)
def clean_peaks_data(
    context: OpExecutionContext, raw_peaks: pd.DataFrame
) -> pd.DataFrame:
    """
    Clean and standardize peaks data.

    Performs the following cleaning operations:
    - Standardize peak names and remove duplicates
    - Convert height measurements to consistent units
    - Clean location and coordinate information
    - Handle climbing status classification
    - Validate and correct elevation data

    Args:
        raw_peaks: Raw peaks DataFrame from extraction

    Returns:
        pd.DataFrame: Cleaned peaks data ready for transformation
    """
    config: ETLConfigResource = context.resources.etl_config

    context.log.info("Starting peaks data cleaning")

    if raw_peaks.empty:
        context.log.warning("Empty peaks DataFrame received")
        return raw_peaks

    df = raw_peaks.copy()
    initial_rows = len(df)

    try:
        # 1. Handle duplicate PEAKID records
        duplicate_count = df["PEAKID"].duplicated().sum()
        if duplicate_count > 0:
            context.log.warning(f"Found {duplicate_count} duplicate PEAKID records")
            # Keep record with most complete data
            df["_completeness_score"] = df.notna().sum(axis=1)
            df = df.sort_values(
                ["PEAKID", "_completeness_score"], ascending=[True, False]
            )
            df = df.drop_duplicates(subset=["PEAKID"], keep="first")
            df = df.drop("_completeness_score", axis=1)

        # 2. Clean peak names
        if "PKNAME" in df.columns:
            df["PKNAME"] = _clean_text_field(df["PKNAME"])
            df["PKNAME_CLEANED"] = df["PKNAME"].str.title()

        # 3. Convert and validate height measurements
        height_columns = ["HEIGHTM", "HEIGHTF"]
        for col in height_columns:
            if col in df.columns:
                df[col] = _convert_to_numeric(df[col], "float", context, col)

                # Validate reasonable height ranges for mountains
                if col == "HEIGHTM":  # Height in meters
                    invalid_heights = (df[col] < 1000) | (df[col] > 9000)
                    invalid_count = invalid_heights.sum()
                    if invalid_count > 0:
                        context.log.warning(
                            f"Found {invalid_count} peaks with invalid heights"
                        )
                        df.loc[invalid_heights, col] = np.nan

        # 4. Clean location fields
        location_fields = ["LOCATION", "REGION"]
        for col in location_fields:
            if col in df.columns:
                df[col] = _clean_text_field(df[col])

        # 5. Handle climbing status
        if "PSTATUS" in df.columns:
            df["PSTATUS"] = df["PSTATUS"].astype(str).str.upper().str.strip()
            # Standardize climbing status codes
            status_mapping = {
                "CLIMBED": "Climbed",
                "UNCLIMBED": "Unclimbed",
                "UNKNOWN": "Unknown",
                "NAN": "Unknown",
            }
            df["CLIMBING_STATUS"] = df["PSTATUS"].map(status_mapping).fillna("Unknown")

        # 6. Parse coordinates if available
        coord_fields = ["LAT", "LON", "LATLON"]
        for col in coord_fields:
            if col in df.columns:
                df[col] = _convert_to_numeric(df[col], "float", context, col)

        # 7. Extract first ascent information
        if "YRDSC" in df.columns:
            df["FIRST_ASCENT_YEAR"] = _convert_to_numeric(
                df["YRDSC"], "int", context, "YRDSC"
            )
            # Validate reasonable year range
            current_year = datetime.now().year
            invalid_years = (df["FIRST_ASCENT_YEAR"] < 1800) | (
                df["FIRST_ASCENT_YEAR"] > current_year
            )
            df.loc[invalid_years, "FIRST_ASCENT_YEAR"] = np.nan

        # 8. Handle missing values
        df = _handle_missing_values(df, context)

        # 9. Remove peaks with insufficient data
        # Keep peaks with at least PEAKID and name or height
        valid_peaks = df["PEAKID"].notna() & (
            df.get("PKNAME", "").notna() | df.get("HEIGHTM", 0).notna()
        )

        invalid_count = (~valid_peaks).sum()
        if invalid_count > 0:
            context.log.warning(
                f"Removing {invalid_count} peaks with insufficient data"
            )
            df = df[valid_peaks]

        final_rows = len(df)
        context.log.info(f"Peaks cleaning completed")
        context.log.info(
            f"Rows: {initial_rows} → {final_rows} ({initial_rows - final_rows} removed)"
        )

        return df

    except Exception as e:
        context.log.error(f"Failed to clean peaks data: {str(e)}")
        raise


@op(
    name="clean_world_bank_data",
    description="Clean and transform World Bank indicators data",
    out=Out(pd.DataFrame, description="Cleaned and normalized World Bank DataFrame"),
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
    required_resource_keys={"etl_config"},
)
def clean_world_bank_data(
    context: OpExecutionContext, raw_world_bank: pd.DataFrame
) -> pd.DataFrame:
    """
    Clean and transform World Bank indicators data from wide to long format.

    Performs the following operations:
    - Transform from wide format (years as columns) to long format
    - Clean country codes and names
    - Filter for relevant indicators
    - Handle missing values in indicator data
    - Convert to appropriate data types

    Args:
        raw_world_bank: Raw World Bank DataFrame from extraction

    Returns:
        pd.DataFrame: Cleaned World Bank data in long format
    """
    context.log.info("Starting World Bank data cleaning")

    if raw_world_bank.empty:
        context.log.warning("Empty World Bank DataFrame received")
        return raw_world_bank

    df = raw_world_bank.copy()
    initial_rows = len(df)
    
    try:
        # 1. Identify year columns
        df = df.rename(columns={
            "country_code": "Country Code",
            "country_name": "Country Name",
            "indicator_code": "Series Code",
            "indicator_name": "Series Name",
            "year": "Year",
            "value": "Value"
        })

        # Ensure correct types
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

        # Remove rows with missing values
        initial_long_rows = len(df)
        df = df.dropna(subset=["Value"])
        removed_na = initial_long_rows - len(df)

        context.log.info(f"Removed {removed_na} rows with missing indicator values")

        # Filter for reasonable years (optional - keep all years >= 1960)
        df = df[df["Year"] >= 1960]

        # Add data source metadata
        df["Data_Source"] = "World Bank"
        df["Last_Updated"] = datetime.now()

        # Sort by country, indicator, and year
        df = df.sort_values(["Country Code", "Series Code", "Year"])

        final_rows = len(df)
        context.log.info(f"World Bank data cleaning completed")
        context.log.info(f"Original records: {initial_rows}")
        context.log.info(f"Final records: {final_rows}")
        context.log.info(f"Unique countries: {df['Country Code'].nunique()}")
        context.log.info(f"Unique indicators: {df['Series Code'].nunique()}")
        context.log.info(f"Year range: {df['Year'].min()} - {df['Year'].max()}")

        return df
    except Exception as e:
        context.log.error(f"Failed to clean World Bank data: {str(e)}")
        raise


def _clean_date_column(
    series: pd.Series, context: OpExecutionContext, col_name: str
) -> pd.Series:
    """Clean and convert date column to datetime format."""
    try:
        # Try multiple date formats
        date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y", "%m/%Y"]

        cleaned_series = pd.to_datetime(
            series, errors="coerce", infer_datetime_format=True
        )

        # If that fails, try explicit formats
        if cleaned_series.isna().all():
            for date_format in date_formats:
                try:
                    cleaned_series = pd.to_datetime(
                        series, format=date_format, errors="coerce"
                    )
                    if not cleaned_series.isna().all():
                        break
                except:
                    continue

        na_count = cleaned_series.isna().sum()
        if na_count > 0:
            context.log.warning(
                f"Could not parse {na_count} dates in column {col_name}"
            )

        return cleaned_series

    except Exception as e:
        context.log.warning(f"Error cleaning date column {col_name}: {str(e)}")
        return series


def _convert_to_numeric(
    series: pd.Series, dtype: str, context: OpExecutionContext, col_name: str
) -> pd.Series:
    """Convert series to numeric type with error handling."""
    try:
        if dtype == "int":
            numeric_series = pd.to_numeric(series, errors="coerce")
            # Convert to nullable integer type
            return numeric_series.astype("Int64")
        elif dtype == "float":
            return pd.to_numeric(series, errors="coerce")
        else:
            return series

    except Exception as e:
        context.log.warning(f"Error converting {col_name} to {dtype}: {str(e)}")
        return series


def _clean_text_field(series: pd.Series) -> pd.Series:
    """Clean text field by removing extra whitespace and standardizing."""
    if series.dtype != "object":
        return series

    cleaned = series.astype(str)
    cleaned = cleaned.str.strip()
    cleaned = cleaned.str.replace(r"\s+", " ", regex=True)  # Multiple spaces to single
    cleaned = cleaned.replace(["nan", "NaN", "NULL", "null", ""], np.nan)

    return cleaned


def _parse_leaders_field(leaders_text: Any) -> List[str]:
    """Parse LEADERS field into list of leader names."""
    if pd.isna(leaders_text) or leaders_text == "":
        return []

    leaders_str = str(leaders_text)

    # Split by common delimiters
    leaders = re.split(r"[,;/&]", leaders_str)

    # Clean each leader name
    cleaned_leaders = []
    for leader in leaders:
        leader = leader.strip()
        if leader and leader.lower() not in ["unknown", "nan", "none"]:
            cleaned_leaders.append(leader)

    return cleaned_leaders


def _determine_expedition_success(df: pd.DataFrame) -> pd.Series:
    """Determine expedition success based on available indicators."""
    success_conditions = []

    # Check if summit members > 0
    if "SMTMEMBERS" in df.columns:
        success_conditions.append(df["SMTMEMBERS"] > 0)

    # Check termination reason
    if "TERMREASON" in df.columns:
        success_reasons = ["SUCCESS (MAIN PEAK)", "SUCCESS (SUBPEAK)", "SUCCESS"]
        success_conditions.append(df["TERMREASON"].isin(success_reasons))

    # If any condition indicates success, mark as successful
    if success_conditions:
        return pd.concat(success_conditions, axis=1).any(axis=1)
    else:
        return pd.Series([False] * len(df), index=df.index)


def _clean_name_field(series: pd.Series) -> pd.Series:
    """Clean name fields with proper capitalization."""
    cleaned = _clean_text_field(series)

    # Proper case for names
    cleaned = cleaned.str.title()

    # Handle common name patterns
    cleaned = cleaned.str.replace(
        r"\bMc(\w)", r"Mc\1", regex=True
    )  # McDonald -> McDonald
    cleaned = cleaned.str.replace(
        r"\bO'(\w)", r"O'\1", regex=True
    )  # O'connor -> O'Connor

    return cleaned


def _create_full_name(first_name: pd.Series, last_name: pd.Series) -> pd.Series:
    """Create full name from first and last name components."""
    first_clean = first_name.fillna("").astype(str).str.strip()
    last_clean = last_name.fillna("").astype(str).str.strip()

    # Combine names
    full_names = []
    for f, l in zip(first_clean, last_clean):
        parts = [part for part in [f, l] if part and part != "nan"]
        full_names.append(" ".join(parts) if parts else np.nan)

    return pd.Series(full_names)


def _create_member_identifier(df: pd.DataFrame) -> pd.Series:
    """Create unique member identifier based on name and expedition."""
    identifiers = []

    for idx, row in df.iterrows():
        parts = []

        if "EXPID" in df.columns and pd.notna(row["EXPID"]):
            parts.append(str(row["EXPID"]))

        if "FULL_NAME" in df.columns and pd.notna(row["FULL_NAME"]):
            # Use initials or short name
            name_parts = str(row["FULL_NAME"]).split()
            if len(name_parts) >= 2:
                parts.append(f"{name_parts[0][:3]}{name_parts[-1][:3]}")

        if parts:
            identifiers.append("_".join(parts))
        else:
            identifiers.append(f"MEMBER_{idx}")

    return pd.Series(identifiers)


def _handle_missing_values(
    df: pd.DataFrame, context: OpExecutionContext
) -> pd.DataFrame:
    """Handle missing values with appropriate strategies."""

    # For numeric columns, fill with 0 or median based on context
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col.lower() in ["deaths", "death", "injury", "injured"]:
            df[col] = df[col].fillna(0)  # Deaths/injuries default to 0
        elif col.lower() in ["members", "totmembers", "smtmembers"]:
            df[col] = df[col].fillna(0)  # Member counts default to 0

    # For text columns, fill with 'Unknown' or leave as NaN
    text_cols = df.select_dtypes(include=["object"]).columns
    for col in text_cols:
        if col.lower() in ["status", "reason", "termreason"]:
            df[col] = df[col].fillna("Unknown")

    return df


def _validate_cleaned_data(df: pd.DataFrame, context: OpExecutionContext):
    """Validate cleaned data quality."""

    # Check for completely empty rows
    empty_rows = df.isnull().all(axis=1).sum()
    if empty_rows > 0:
        context.log.warning(f"Found {empty_rows} completely empty rows")

    # Check key field completeness
    key_fields = ["EXPID", "PEAKID", "YEAR"] if "EXPID" in df.columns else []
    for field in key_fields:
        if field in df.columns:
            missing_count = df[field].isnull().sum()
            if missing_count > 0:
                context.log.warning(
                    f"Key field {field} has {missing_count} missing values"
                )

    context.log.info("Data validation completed")
