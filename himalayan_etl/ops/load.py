from dataclasses import dataclass
from dagster import Field, In, Nothing, OpExecutionContext, Out, RetryPolicy, String, op, Array
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


def get_id_datatype(table_data: pd.DataFrame) -> str:
    datatype = table_data.dtypes.to_dict()["Id"]

    if datatype == "int64":
        datatype = "INT"
    elif datatype == "str":
        datatype = "VARCHAR(255)"
    elif datatype == "object":
        datatype = "VARCHAR(255)"
    else:
        raise ValueError(f"Unsupported datatype for primary key 'Id': {datatype}")

    return datatype


@op(
    name="load_table",
    description="Load table into SQL Server table with upsert logic",
    ins={"table_data": In(pd.DataFrame), "_after": In(Nothing)},
    out=Out(LoadResult, description="Load results"),
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    config_schema=load_table_config_schema,
    required_resource_keys={"db"},
)
def load_table(context: OpExecutionContext, table_data: pd.DataFrame) -> LoadResult:
    db: DatabaseResource = context.resources.db

    table_name = context.op_config["table_name"]

    context.log.info(f"Loading table: {table_name} with {len(table_data)} records")

    try:
        if db.table_exists(context, table_name):
            context.log.info(f"Dropping existing table: {table_name}")
            db.drop_table(context, table_name)

        rows_affected = db.bulk_insert(
            context=context,
            df=table_data,
            table_name=table_name,
        )

        db.set_pk(context, table_name, "Id", get_id_datatype(table_data))

        context.log.info(f"Successfully loaded {rows_affected} records to {table_name}")

        return LoadResult(
            table_name=table_name,
            rows_affected=rows_affected,
            total_records=len(table_data),
            status="success",
        )
    except Exception as e:
        raise Exception(f"Failed to load table_data {table_name}: {str(e)}")

@op(
    name="drop_all_fk",
    description="Drop all foreign keys from a specified table",
    ins={"_after": In(Nothing)},
    required_resource_keys={"db"},
)
def drop_all_fk(context: OpExecutionContext):
    db: DatabaseResource = context.resources.db
    table_name = "FACT_MemberExpedition"

    context.log.info(f"Dropping all foreign keys from table: {table_name}")

    try:
        db.drop_fk(context, table_name, "PeakId")
        db.drop_fk(context, table_name, "ExpeditionId")
        db.drop_fk(context, table_name, "DateId")
        db.drop_fk(context, table_name, "CountryIndicatorId")
        context.log.info(f"Successfully dropped all foreign keys from {table_name}")
    except Exception as e:
        raise Exception(f"Failed to drop foreign keys from {table_name}: {str(e)}")


load_dim_date = load_table.configured({"table_name": "DIM_Date"}, name="load_dim_date")

load_dim_peak = load_table.configured({"table_name": "DIM_Peak"}, name="load_dim_peak")

load_dim_country_indicator = load_table.configured(
    {"table_name": "DIM_CountryIndicator"},
    name="load_dim_country_indicator",
)

load_dim_expedition = load_table.configured(
    {"table_name": "DIM_Expedition"}, name="load_dim_expedition"
)



@op(
    name="load_fact_member_expedition_table",
    description="Load table into SQL Server table with upsert logic",
    ins={"table_data": In(pd.DataFrame), "_after": In(Nothing)},
    out=Out(LoadResult, description="Load results"),
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    required_resource_keys={"db"},
)
def load_fact_member_expedition(
    context: OpExecutionContext, table_data: pd.DataFrame, table_name: str = "FACT_MemberExpedition"
) -> LoadResult:
    db: DatabaseResource = context.resources.db

    context.log.info(f"Loading table: {table_name} with {len(table_data)} records")

    table_name = "FACT_MemberExpedition"

    try:
        if db.table_exists(context, table_name):
            context.log.info(f"Dropping existing table: {table_name}")
            db.drop_table(context, table_name)

        rows_affected = db.bulk_insert(
            context=context,
            df=table_data,
            table_name=table_name,
        )

        context.log.info(f"Successfully loaded {rows_affected} records to {table_name}")

        db.set_pk(context, table_name, "Id", get_id_datatype(table_data))

        schema = db.get_table_schema(context, table_name)
        context.log.info(f"Table {table_name} schema: {schema}")
        context.log.info(f"Table DIM_Date schema: {db.get_table_schema(context, 'DIM_Date')}")

        db.set_type(context, table_name, "PeakId", "VARCHAR(255)")
        db.set_fk(context, table_name, "PeakId", "DIM_Peak", "Id")

        db.set_type(context, table_name, "ExpeditionId", "VARCHAR(255)")
        db.set_fk(context, table_name, "ExpeditionId", "DIM_Expedition", "Id")

        db.set_type(context, table_name, "DateId", "INT")
        db.set_fk(context, table_name, "DateId", "DIM_Date", "Id")

        db.set_type(context, table_name, "CountryIndicatorId", "INT")
        db.set_fk(context, table_name, "CountryIndicatorId", "DIM_CountryIndicator", "Id")

        return LoadResult(
            table_name=table_name,
            rows_affected=rows_affected,
            total_records=len(table_data),
            status="success",
        )
    except Exception as e:
        raise Exception(f"Failed to load table_data {table_name}: {str(e)}")
