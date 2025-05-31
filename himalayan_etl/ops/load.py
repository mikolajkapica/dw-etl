from dataclasses import dataclass
from dagster import Field, In, OpExecutionContext, Out, RetryPolicy, String, op, Array
import pandas as pd

from himalayan_etl.resources import DatabaseResource, ETLConfigResource


load_table_config_schema = {
    "table_name": Field(String, description="Target table name in SQL Server"),
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
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    config_schema=load_table_config_schema,
    required_resource_keys={"db"},
)
def load_table(context: OpExecutionContext, table_data: pd.DataFrame) -> LoadResult:
    db: DatabaseResource = context.resources.db

    table_name = context.op_config["table_name"]

    context.log.info(f"Loading table: {table_name}")

    try:
        rows_affected = db.bulk_insert(
            df=table_data,
            table_name=table_name,
        )

        context.log.info(f"Successfully loaded {rows_affected} records to {table_name}")

        return LoadResult(
            table_name=table_name,
            rows_affected=rows_affected,
            total_records=len(table_data),
            status="success",
        )
    except Exception as e:
        raise Exception(
            f"Failed to load table_data {table_name}: {str(e)}"
        )


load_dim_date = load_table.configured({"table_name": "DIM_Date"}, name="load_dim_date")

load_dim_peak = load_table.configured({"table_name": "DIM_Peak"}, name="load_dim_peak")

load_dim_country_indicator = load_table.configured(
    {"table_name": "DIM_CountryIndicator"},
    name="load_dim_country_indicator",
)

load_dim_expedition = load_table.configured(
    {"table_name": "DIM_Expedition"}, name="load_dim_expedition"
)

load_fact_member_expedition = load_table.configured(
    {"table_name": "FACT_MemberExpedition"}, name="load_fact_member_expedition"
)
