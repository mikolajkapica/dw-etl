import uuid
from fuzzywuzzy import process
import pandas as pd
from dagster import In, op, Out, OpExecutionContext


@op(
    name="transform_members_data",
    description="Transform and standardize members data",
    ins={
        "raw_members": In(pd.DataFrame),
        "dim_country_indicators": In(pd.DataFrame),
        "dim_date": In(pd.DataFrame),
        "dim_expedition": In(pd.DataFrame),
        "dim_route": In(pd.DataFrame),
    },
    out=Out(pd.DataFrame, description="Transformed members DataFrame"),
)
def transform_members_data(
    context: OpExecutionContext,
    raw_members: pd.DataFrame,
    dim_country_indicators: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_expedition: pd.DataFrame,
) -> pd.DataFrame:
    context.log.info("Starting members data transformation")
    context.log.info(f"Raw members data sample:\n{raw_members.head()}")
    context.log.info(f"Raw members data columns: {raw_members.columns.tolist()}")

    # Raw members data columns: ['EXPID', 'MEMBID', 'PEAKID', 'MYEAR', 'MSEASON', 'FNAME', 'LNAME', 'SEX', 'AGE', 'BIRTHDATE', 'YOB', 'CALCAGE', 'CITIZEN', 'STATUS', 'RESIDENCE', 'OCCUPATION', 'LEADER', 'DEPUTY', 'BCONLY', 'NOTTOBC', 'SUPPORT', 'DISABLED', 'HIRED', 'SHERPA', 'TIBETAN', 'MSUCCESS', 'MCLAIMED', 'MDISPUTED', 'MSOLO', 'MTRAVERSE', 'MSKI', 'MPARAPENTE', 'MSPEED', 'MHIGHPT', 'MPERHIGHPT', 'MSMTDATE1', 'MSMTDATE2', 'MSMTDATE3', 'MSMTTIME1', 'MSMTTIME2', 'MSMTTIME3', 'MROUTE1', 'MROUTE2', 'MROUTE3', 'MASCENT1', 'MASCENT2', 'MASCENT3', 'MO2USED', 'MO2NONE', 'MO2CLIMB', 'MO2DESCENT', 'MO2SLEEP', 'MO2MEDICAL', 'MO2NOTE', 'DEATH', 'DEATHDATE', 'DEATHTIME', 'DEATHTYPE', 'DEATHHGTM', 'DEATHCLASS', 'AMS', 'WEATHER', 'INJURY', 'INJURYDATE', 'INJURYTIME', 'INJURYTYPE', 'INJURYHGTM', 'DEATHNOTE', 'MEMBERMEMO', 'NECROLOGY', 'MSMTBID', 'MSMTTERM', 'HCN', 'MCHKSUM', 'MSMTNOTE1', 'MSMTNOTE2', 'MSMTNOTE3', 'DEATHRTE']

    with_date = pd.merge(
        raw_members,
        dim_date.rename(columns={"Id": "DateId"}),
        left_on=["MYEAR", "MSEASON"],
        right_on=["Year", "Quarter"],
        how="left",
    )

    context.log.info(f"Data after merging with dim_date:\n{with_date.head()}")

    countries = dim_country_indicators["CountryName"].unique()
    couldnt_match = []
    country_match_cache = {}

    def fuzzy_match_country(x):
        if not isinstance(x, str):
            nonlocal couldnt_match
            couldnt_match.append(x)
            return None
        if x in country_match_cache:
            return country_match_cache[x]
        if x in countries:
            country_match_cache[x] = x
        else:
            match = process.extractOne(x, countries)
            country_match_cache[x] = match[0]
        return country_match_cache[x]

    with_date["CITIZEN"] = with_date["CITIZEN"].apply(fuzzy_match_country)

    context.log.info(f"Countries that couldn't be matched: {couldnt_match}")

    with_country_indicators = pd.merge(
        with_date,
        dim_country_indicators.rename(columns={"Id": "CountryIndicatorId"}),
        left_on=["CITIZEN", "MYEAR"],
        right_on=["CountryName", "Year"],
        how="left",
    )

    context.log.info(
        f"Data after merging with dim_country_indicators and dim_route:\n{with_country_indicators.head()}"
    )

    # columns
    context.log.info(f"Columns after merging:\n{with_country_indicators.columns.tolist()}")

    # pick columns we need
    raw_members = with_country_indicators[
        [
            "EXPID",
            "PEAKID",
            "FNAME",
            "LNAME",
            "YOB",
            "SEX",
            "CITIZEN",
            "MSUCCESS",
            "AGE",
            "DEATH",
            "MO2USED",
            "HIRED",
            "DateId",
            "CountryIndicatorId",
        ]
    ]

    raw_members = raw_members.rename(
        columns={
            "EXPID": "ExpeditionId",
            "PEAKID": "PeakId",
            "FNAME": "FirstName",
            "LNAME": "LastName",
            "YOB": "YearOfBirth",
            "SEX": "Gender",
            "CITIZEN": "CitizenshipCountry",
            "AGE": "AgeGroup",
            "MSUCCESS": "Success",
            "MO2USED": "OxygenUsed",
            "HIRED": "Hired",
            "DEATH": "Death",
        }
    )

    age_bins = [0, 18, 30, 40, 50, 60, 70, 80, 90, 100]
    age_labels = [
        "0-17",
        "18-29",
        "30-39",
        "40-49",
        "50-59",
        "60-69",
        "70-79",
        "80-89",
        "90+",
    ]
    raw_members["AgeGroup"] = pd.cut(
        raw_members["AgeGroup"],
        bins=age_bins,
        labels=age_labels,
        right=False,
    )

    raw_members.insert(0, "Id", range(1, len(raw_members) + 1))

    raw_members["ExpeditionId"] = raw_members["ExpeditionId"].astype(str)
    raw_members["PeakId"] = raw_members["PeakId"].astype(str)
    raw_members["FirstName"] = raw_members["FirstName"].astype(str)
    raw_members["LastName"] = raw_members["LastName"].astype(str)
    raw_members["YearOfBirth"] = pd.to_numeric(raw_members["YearOfBirth"], errors="raise")
    raw_members["Gender"] = raw_members["Gender"].astype(str)
    raw_members["CitizenshipCountry"] = raw_members["CitizenshipCountry"].astype(str)
    raw_members["AgeGroup"] = raw_members["AgeGroup"].astype(str)
    raw_members["Success"] = pd.to_numeric(raw_members["Success"], errors="raise")

    return raw_members


@op(
    name="transform_expeditions_data",
    description="Transform and standardize expeditions data",
    ins={"raw_expeditions": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Transformed expeditions DataFrame"),
)
def transform_expeditions_data(
    context: OpExecutionContext, raw_expeditions: pd.DataFrame
) -> pd.DataFrame:
    context.log.info("Starting expeditions data transformation")
    context.log.info(f"Raw expeditions data sample:\n{raw_expeditions.head()}")
    context.log.info(f"Raw expeditions data columns: {raw_expeditions.columns.tolist()}")

    # Raw expeditions data columns: ['EXPID', 'PEAKID', 'YEAR', 'SEASON', 'HOST', 'ROUTE1', 'ROUTE2', 'ROUTE3', 'ROUTE4', 'NATION', 'LEADERS', 'SPONSOR', 'SUCCESS1', 'SUCCESS2', 'SUCCESS3', 'SUCCESS4', 'ASCENT1', 'ASCENT2', 'ASCENT3', 'ASCENT4', 'CLAIMED', 'DISPUTED', 'COUNTRIES', 'APPROACH', 'BCDATE', 'SMTDATE', 'SMTTIME', 'SMTDAYS', 'TOTDAYS', 'TERMDATE', 'TERMREASON', 'TERMNOTE', 'HIGHPOINT', 'TRAVERSE', 'SKI', 'PARAPENTE', 'CAMPS', 'ROPE', 'TOTMEMBERS', 'SMTMEMBERS', 'MDEATHS', 'TOTHIRED', 'SMTHIRED', 'HDEATHS', 'NOHIRED', 'O2USED', 'O2NONE', 'O2CLIMB', 'O2DESCENT', 'O2SLEEP', 'O2MEDICAL', 'O2TAKEN', 'O2UNKWN', 'OTHERSMTS', 'CAMPSITES', 'ROUTEMEMO', 'ACCIDENTS', 'ACHIEVMENT', 'AGENCY', 'COMRTE', 'STDRTE', 'PRIMRTE', 'PRIMMEM', 'PRIMREF', 'PRIMID', 'CHKSUM']

    transformed_expeditions = raw_expeditions[
        [
            "EXPID",
            "HOST",
            "ROUTE1",
            "SUCCESS1",
        ]
    ]

    transformed_expeditions = transformed_expeditions.rename(
        columns={
            "EXPID": "Id",
            "HOST": "Host",
            "ROUTE1": "Route",
            "SUCCESS1": "Success",
        }
    )

    transformed_expeditions = transformed_expeditions.drop_duplicates(subset=["Id"]).reset_index(
        drop=True
    )

    transformed_expeditions["Id"] = transformed_expeditions["Id"].astype(str)
    transformed_expeditions["Host"] = pd.to_numeric(transformed_expeditions["Host"], errors="raise")
    transformed_expeditions["Route"] = transformed_expeditions["Route"].astype(str)
    transformed_expeditions["Success"] = pd.to_numeric(
        transformed_expeditions["Success"], errors="raise"
    )

    return transformed_expeditions


@op(
    name="transform_peaks_data",
    description="Transform and standardize peaks data",
    ins={"raw_peaks": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Transformed peaks DataFrame"),
)
def transform_peaks_data(context: OpExecutionContext, raw_peaks: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Starting peaks data transformation")
    context.log.info(f"Raw peaks data sample:\n{raw_peaks.head()}")
    context.log.info(f"Raw peaks data columns: {raw_peaks.columns.tolist()}")

    # Raw peaks data columns: ['PEAKID', 'PKNAME', 'PKNAME2', 'LOCATION', 'HEIGHTM', 'HEIGHTF', 'HIMAL', 'REGION', 'OPEN', 'UNLISTED', 'TREKKING', 'TREKYEAR', 'RESTRICT', 'PHOST', 'PSTATUS', 'PEAKMEMO', 'PYEAR', 'PSEASON', 'PEXPID', 'PSMTDATE', 'PCOUNTRY', 'PSUMMITERS', 'PSMTNOTE', 'REFERMEMO', 'PHOTOMEMO']

    raw_peaks = raw_peaks[
        [
            "PEAKID",
            "PKNAME",
            "HEIGHTM",
        ]
    ]

    raw_peaks = raw_peaks.rename(
        columns={
            "PEAKID": "Id",
            "PKNAME": "Name",
            "HEIGHTM": "HeightMeters",
        }
    )

    raw_peaks["Id"] = raw_peaks["Id"].astype(str)
    raw_peaks["Name"] = raw_peaks["Name"].astype(str)
    raw_peaks["HeightMeters"] = pd.to_numeric(raw_peaks["HeightMeters"], errors="raise")

    height_bins = [5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000]
    height_labels = [
        "5000-5499",
        "5500-5999",
        "6000-6499",
        "6500-6999",
        "7000-7499",
        "7500-7999",
        "8000-8499",
        "8500-8999",
    ]
    raw_peaks["HeightCategory"] = pd.cut(
        raw_peaks["HeightMeters"],
        bins=height_bins,
        labels=height_labels,
        right=False,
    )

    return raw_peaks


@op(
    name="transform_world_bank_data",
    description="Transform and standardize World Bank indicators data",
    ins={"raw_world_bank": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Cleaned and normalized World Bank DataFrame"),
)
def transform_world_bank_data(
    context: OpExecutionContext, raw_world_bank: pd.DataFrame
) -> pd.DataFrame:
    context.log.info("Starting World Bank data transformation")
    context.log.info(f"Raw World Bank data sample:\n{raw_world_bank.head()}")
    context.log.info(f"Raw World Bank data columns: {raw_world_bank.columns.tolist()}")

    # Raw World Bank data columns: ['COUNTRYCODE', 'COUNTRYNAME', 'INDICATORCODE', 'YEAR', 'VALUE']

    transformed_world_bank = raw_world_bank.pivot_table(
        index=["COUNTRYCODE", "COUNTRYNAME", "YEAR"], columns="INDICATORCODE", values="VALUE"
    ).reset_index()

    indicators = [
        "NY.GDP.PCAP.CD",  # GDP per capita (current US$)
        "HD.HCI.OVRL",  # Human Capital Index (HCI) overall
        "IT.NET.USER.ZS",  # Individuals using the Internet (% of population)
        "SH.MED.PHYS.ZS",  # Physicians (per 1,000 people)
        "PV.EST",  # Political Stability and Absence of Violence
    ]
    columns_to_keep = ["COUNTRYCODE", "COUNTRYNAME", "YEAR"] + indicators
    transformed_world_bank = transformed_world_bank[columns_to_keep]

    for indicator in indicators:
        first_idx = transformed_world_bank.groupby("COUNTRYCODE").head(1).index
        null_first = transformed_world_bank.loc[first_idx, indicator].isnull()
        transformed_world_bank.loc[first_idx[null_first], indicator] = 0

        transformed_world_bank[indicator] = transformed_world_bank.groupby("COUNTRYCODE")[
            indicator
        ].transform(lambda x: x.interpolate())

    transformed_world_bank = transformed_world_bank.rename(
        columns={
            "COUNTRYCODE": "CountryCode",
            "COUNTRYNAME": "CountryName",
            "YEAR": "Year",
            "NY.GDP.PCAP.CD": "GDPPerCapita",
            "HD.HCI.OVRL": "HumanCapitalIndex",
            "IT.NET.USER.ZS": "InternetUsersPercentage",
            "SH.MED.PHYS.ZS": "PhysiciansPer1000People",
            "PV.EST": "PoliticalStabilityIndex",
        }
    )

    transformed_world_bank.insert(0, "Id", transformed_world_bank.index + 1)

    transformed_world_bank["CountryCode"] = transformed_world_bank["CountryCode"].astype(str)
    transformed_world_bank["CountryName"] = transformed_world_bank["CountryName"].astype(str)
    transformed_world_bank["Year"] = pd.to_numeric(transformed_world_bank["Year"], errors="raise")
    transformed_world_bank["GDPPerCapita"] = pd.to_numeric(
        transformed_world_bank["GDPPerCapita"], errors="raise"
    )
    transformed_world_bank["HumanCapitalIndex"] = pd.to_numeric(
        transformed_world_bank["HumanCapitalIndex"], errors="raise"
    )
    transformed_world_bank["InternetUsersPercentage"] = pd.to_numeric(
        transformed_world_bank["InternetUsersPercentage"], errors="raise"
    )
    transformed_world_bank["PhysiciansPer1000People"] = pd.to_numeric(
        transformed_world_bank["PhysiciansPer1000People"], errors="raise"
    )
    transformed_world_bank["PoliticalStabilityIndex"] = pd.to_numeric(
        transformed_world_bank["PoliticalStabilityIndex"], errors="raise"
    )

    context.log.info(f"Transformed World Bank data sample:\n{transformed_world_bank.head()}")
    return transformed_world_bank


@op(
    name="create_dim_date",
    description="Create dimension date DataFrame",
    out=Out(pd.DataFrame, description="Dimension date DataFrame"),
)
def create_dim_date(context: OpExecutionContext, raw_members: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Creating dimension date DataFrame")

    dim_date = pd.DataFrame(
        {
            "Year": raw_members["MYEAR"],
            "Season": raw_members["MSEASON"],
        }
    )

    dim_date = dim_date.drop_duplicates(subset=["Year", "Season"]).reset_index(drop=True)

    dim_date.insert(0, "Id", dim_date.index + 1)

    dim_date["Year"] = pd.to_numeric(dim_date["Year"], errors="raise")
    dim_date["Season"] = pd.to_numeric(dim_date["Season"], errors="raise")

    dim_date["Quarter"] = dim_date["Season"]

    dim_date = dim_date[["Id", "Year", "Quarter"]]

    return dim_date
