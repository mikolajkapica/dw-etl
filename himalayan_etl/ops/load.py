from dataclasses import dataclass
from dagster import Field, In, OpExecutionContext, Out, RetryPolicy, String, op, Array
import pandas as pd

from himalayan_etl.resources import DatabaseResource, ETLConfigResource


load_table_config_schema = {
    "table_name": Field(String, description="Target table name in SQL Server"),
    "key_columns": Field(
        Array(String), default_value=[], description="Columns used to identify existing records"
    ),
}


@dataclass
class LoadResult:
    table_name: str
    rows_affected: int
    total_records: int
    status: str


@op(
    name="load_table",
    description="Load table into SQL Server table with upsert logic",
    ins={"table_data": In(pd.DataFrame)},
    out=Out(LoadResult, description="Load results"),
    config_schema=load_table_config_schema,
    required_resource_keys={"db"},
)
def load_table(context: OpExecutionContext, table_data: pd.DataFrame) -> LoadResult:
    db: DatabaseResource = context.resources.db

    table_name = context.op_config["table_name"]
    key_columns = context.op_config["key_columns"]
    update_columns = context.op_config["update_columns"]

    context.log.info(f"Loading table: {table_name}")

    try:
        if not update_columns:
            update_columns = [col for col in table_data.columns if col not in key_columns]

        rows_affected = db.bulk_insert(
            df=table_data,
            table_name=table_name,
            key_columns=key_columns,
        )

        context.log.info(f"Successfully loaded {rows_affected} records to {table_name}")

        return LoadResult(
            table_name=table_name,
            rows_affected=rows_affected,
            total_records=len(table_data),
            status="success",
        )
    except Exception as e:
        context.log.error(f"Failed to load table_data {table_name}: {str(e)}")
        return LoadResult(
            table_name=table_name,
            rows_affected=0,
            total_records=0,
            status="failed",
        )


load_dim_date = load_table.configured(
    {"table_name": "DIM_Date", "key_columns": ["Id"]}, name="load_dim_date"
)

load_dim_peak = load_table.configured(
    {"table_name": "DIM_Peak", "key_columns": ["Id"]}, name="load_dim_peak"
)

load_dim_country_indicator = load_table.configured(
    {
        "table_name": "DIM_CountryIndicator",
        "key_columns": ["Id"],
    },
    name="load_dim_country_indicator",
)

load_dim_expedition = load_table.configured(
    {
        "table_name": "DIM_Expedition",
        "key_columns": ["Id"],
    },
    name="load_dim_expedition",
)

load_fact_member_expedition = load_table.configured(
    {"table_name": "FACT_MemberExpedition", "key_columns": ["Id"]}, name="load_fact_member_expedition"
)
