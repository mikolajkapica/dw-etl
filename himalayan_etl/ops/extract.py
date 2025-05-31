import numpy as np
import pandas as pd
import requests
from dagster import Array
from dagster import Backoff, Jitter
from dagster import OpExecutionContext, Out, RetryPolicy, op
from himalayan_etl.resources import (
    ETLConfigResource,
    FileSystemResource,
    WorldBankConfig,
)

retry_policy = RetryPolicy(max_retries=3, delay=1.0)


@op(
    name="extract_members_data",
    description="Extract expedition members data from CSV file",
    out=Out(pd.DataFrame, description="Raw members DataFrame"),
    retry_policy=retry_policy,
    required_resource_keys={"fs"},
)
def extract_members_data(context: OpExecutionContext) -> pd.DataFrame:
    filesystem: FileSystemResource = context.resources.fs
    context.log.info("Starting members data extraction")

    df = filesystem.read_csv(filename="members.csv")
    if df.empty:
        raise ValueError("Members file is empty")
    context.log.info(f"Loaded {len(df)} member records")

    required_columns = [
        "EXPID",
        "MEMBID",
        "PEAKID",
        "MYEAR",
        "MSEASON",
        "FNAME",
        "LNAME",
        "SEX",
        "AGE",
        "BIRTHDATE",
        "YOB",
        "CALCAGE",
        "CITIZEN",
        "STATUS",
        "RESIDENCE",
        "OCCUPATION",
        "LEADER",
        "DEPUTY",
        "BCONLY",
        "NOTTOBC",
        "SUPPORT",
        "DISABLED",
        "HIRED",
        "SHERPA",
        "TIBETAN",
        "MSUCCESS",
        "MCLAIMED",
        "MDISPUTED",
        "MSOLO",
        "MTRAVERSE",
        "MSKI",
        "MPARAPENTE",
        "MSPEED",
        "MHIGHPT",
        "MPERHIGHPT",
        "MSMTDATE1",
        "MSMTDATE2",
        "MSMTDATE3",
        "MSMTTIME1",
        "MSMTTIME2",
        "MSMTTIME3",
        "MROUTE1",
        "MROUTE2",
        "MROUTE3",
        "MASCENT1",
        "MASCENT2",
        "MASCENT3",
        "MO2USED",
        "MO2NONE",
        "MO2CLIMB",
        "MO2DESCENT",
        "MO2SLEEP",
        "MO2MEDICAL",
        "MO2NOTE",
        "DEATH",
        "DEATHDATE",
        "DEATHTIME",
        "DEATHTYPE",
        "DEATHHGTM",
        "DEATHCLASS",
        "AMS",
        "WEATHER",
        "INJURY",
        "INJURYDATE",
        "INJURYTIME",
        "INJURYTYPE",
        "INJURYHGTM",
        "DEATHNOTE",
        "MEMBERMEMO",
        "NECROLOGY",
        "MSMTBID",
        "MSMTTERM",
        "HCN",
        "MCHKSUM",
        "MSMTNOTE1",
        "MSMTNOTE2",
        "MSMTNOTE3",
        "DEATHRTE",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    return df


@op(
    name="extract_expeditions_data",
    description="Extract expedition data from CSV file",
    out=Out(pd.DataFrame, description="Raw expeditions DataFrame"),
    required_resource_keys={"fs"},
)
def extract_expeditions_data(context: OpExecutionContext) -> pd.DataFrame:
    filesystem: FileSystemResource = context.resources.fs
    context.log.info("Starting expeditions data extraction")

    df = filesystem.read_csv(filename="expeditions.csv")
    if df.empty:
        raise ValueError("Expeditions file is empty")
    context.log.info(f"Loaded {len(df)} expedition records")

    required_columns = [
        "EXPID",
        "PEAKID",
        "YEAR",
        "SEASON",
        "HOST",
        "ROUTE1",
        "ROUTE2",
        "ROUTE3",
        "ROUTE4",
        "NATION",
        "LEADERS",
        "SPONSOR",
        "SUCCESS1",
        "SUCCESS2",
        "SUCCESS3",
        "SUCCESS4",
        "ASCENT1",
        "ASCENT2",
        "ASCENT3",
        "ASCENT4",
        "CLAIMED",
        "DISPUTED",
        "COUNTRIES",
        "APPROACH",
        "BCDATE",
        "SMTDATE",
        "SMTTIME",
        "SMTDAYS",
        "TOTDAYS",
        "TERMDATE",
        "TERMREASON",
        "TERMNOTE",
        "HIGHPOINT",
        "TRAVERSE",
        "SKI",
        "PARAPENTE",
        "CAMPS",
        "ROPE",
        "TOTMEMBERS",
        "SMTMEMBERS",
        "MDEATHS",
        "TOTHIRED",
        "SMTHIRED",
        "HDEATHS",
        "NOHIRED",
        "O2USED",
        "O2NONE",
        "O2CLIMB",
        "O2DESCENT",
        "O2SLEEP",
        "O2MEDICAL",
        "O2TAKEN",
        "O2UNKWN",
        "OTHERSMTS",
        "CAMPSITES",
        "ROUTEMEMO",
        "ACCIDENTS",
        "ACHIEVMENT",
        "AGENCY",
        "COMRTE",
        "STDRTE",
        "PRIMRTE",
        "PRIMMEM",
        "PRIMREF",
        "PRIMID",
        "CHKSUM",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    return df


@op(
    name="extract_peaks_data",
    description="Extract peaks data from CSV file",
    out=Out(pd.DataFrame, description="Raw peaks DataFrame"),
    required_resource_keys={"fs"},
)
def extract_peaks_data(context: OpExecutionContext) -> pd.DataFrame:
    filesystem: FileSystemResource = context.resources.fs
    context.log.info("Starting peaks data extraction")

    df = filesystem.read_csv(filename="peaks.csv")
    if df.empty:
        raise ValueError("Peaks file is empty")
    context.log.info(f"Loaded {len(df)} peak records")

    required_columns = [
        "PEAKID",
        "PKNAME",
        "PKNAME2",
        "LOCATION",
        "HEIGHTM",
        "HEIGHTF",
        "HIMAL",
        "REGION",
        "OPEN",
        "UNLISTED",
        "TREKKING",
        "TREKYEAR",
        "RESTRICT",
        "PHOST",
        "PSTATUS",
        "PEAKMEMO",
        "PYEAR",
        "PSEASON",
        "PEXPID",
        "PSMTDATE",
        "PCOUNTRY",
        "PSUMMITERS",
        "PSMTNOTE",
        "REFERMEMO",
        "PHOTOMEMO",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    return df


@op(
    out=Out(pd.DataFrame, description="Raw World Bank data"),
    description="Extract World Development Indicators data from World Bank API",
    required_resource_keys={"world_bank_config"},
)
def extract_world_bank_data(context) -> pd.DataFrame:
    context.log.info(f"Starting World Bank data extraction")
    world_bank_config: WorldBankConfig = context.world_bank_config

    all_data = []

    for indicator in world_bank_config.indicators:
        context.log.info(f"Fetching data for indicator: {indicator}")

        url = f"{world_bank_config.base_url}/country/all/indicator/{indicator}"
        params = {
            "format": "json",
            "date": f"{world_bank_config.start_year}:{world_bank_config.end_year}",
            "per_page": world_bank_config.max_page_size,
            "page": 1,
        }

        response = requests.get(url, params=params, timeout=world_bank_config.timeout)
        response.raise_for_status()
        data = response.json()
        records = data[1]

        for record in records:
            all_data.append(
                {
                    "COUNTRYCODE": record["country"]["id"],
                    "COUNTRYNAME": record["country"]["value"],
                    "INDICATORNAME": record["indicator"]["id"],
                    "INDICATORNAME": record["indicator"]["value"],
                    "YEAR": int(record["date"]),
                    "VALUE": (float(record["value"]) if record["value"] else None),
                }
            )

    context.log.info(f"Extracted {len(all_data)} total World Bank records")
    return pd.DataFrame(all_data)
