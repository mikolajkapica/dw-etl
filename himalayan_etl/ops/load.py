from dagster import Field, In, OpExecutionContext, Out, RetryPolicy, String, op, Array
import pandas as pd

from himalayan_etl.resources import DatabaseResource, ETLConfigResource
from himalayan_etl.ops.transform import world_bank_config_schema, load_dimension_config_schema


# Configuration schema for load_dimension_table op
load_dimension_config_schema = {
    "table_name": Field(String, description="Target table name in SQL Server"),
    "key_columns": Field(Array(String), default_value=[], description="Columns used to identify existing records"),
    "update_columns": Field(Array(String), default_value=[], description="Columns to update (empty for all non-key columns)"),
}


@op(
    name="load_dimension_table",
    description="Load dimension data into SQL Server table with upsert logic",
    out=Out(dict[str, any], description="Load results"),
    config_schema=load_dimension_config_schema,
    retry_policy=RetryPolicy(max_retries=3, delay=2.0),
    required_resource_keys={"db", "etl_config"},
)
def load_dimension_table(
    context: OpExecutionContext,
    dimension_data: pd.DataFrame
) -> dict[str, any]:
    db: DatabaseResource = context.resources.db
    config: ETLConfigResource = context.resources.etl_config
    
    # Get configuration values
    table_name = context.op_config["table_name"]
    key_columns = context.op_config["key_columns"]
    update_columns = context.op_config["update_columns"]
    
    context.log.info(f"Loading dimension table: {table_name}")
    
    if dimension_data.empty:
        context.log.warning(f"No data to load for {table_name}")
        return {"table_name": table_name, "rows_affected": 0, "status": "no_data"}
    
    try:
        # Prepare update columns if not specified
        if not update_columns:
            update_columns = [col for col in dimension_data.columns if col not in key_columns]
        
        # Perform upsert operation
        rows_affected = db.upsert_dimension(
            df=dimension_data,
            table_name=table_name,
            key_columns=key_columns,
            update_columns=update_columns
        )
        
        context.log.info(f"Successfully loaded {rows_affected} records to {table_name}")
        
        return {
            "table_name": table_name,
            "rows_affected": rows_affected,
            "total_records": len(dimension_data),
            "status": "success"
        }
        
    except Exception as e:
        context.log.error(f"Failed to load dimension table {table_name}: {str(e)}")
        return {
            "table_name": table_name,
            "rows_affected": 0,
            "status": "failed",
            "error": str(e)
        }


@op(
    ins={"dim_country_indicators": In(pd.DataFrame)},
    out=Out(dict[str, any], description="Load results"),
    config_schema=world_bank_config_schema,
    retry_policy=RetryPolicy(max_retries=3, delay=2),
    description="Load country indicators dimension to database"
)
def load_dim_country_indicators(
    context,
    dim_country_indicators: pd.DataFrame,
    db: DatabaseResource
) -> dict[str, any]:
    """
    Load the country indicators dimension to the database.
    """
    
    if dim_country_indicators.empty:
        context.log.warning("No country indicators data to load")
        return {'status': 'skipped', 'reason': 'No data to load'}
    
    context.log.info(f"Loading {len(dim_country_indicators)} country indicator records")
    
    try:
        # Use upsert to handle updates       
        load_result = db.upsert_dataframe(
            dataframe=dim_country_indicators,
            table_name='DIM_CountryIndicators',
            key_columns=['COUNTRY_CODE', 'YEAR'],
            batch_size=context.op_config['batch_size']
        )
        
        context.log.info(f"Successfully loaded country indicators dimension: {load_result}")
        
        return {
            'table_name': 'DIM_CountryIndicators',
            'records_loaded': len(dim_country_indicators),
            'load_timestamp': pd.Timestamp.now().isoformat(),
            'status': 'success'
        }
        
    except Exception as e:
        context.log.error(f"Error loading country indicators dimension: {str(e)}")
        raise