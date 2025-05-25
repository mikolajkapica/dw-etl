"""
Main entry point for the Himalayan Expeditions ETL system.
Provides CLI interface and Dagster repository definition.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, List
import click
from dagster import (
    Definitions,
    DefaultScheduleStatus,
    load_assets_from_modules,
    EnvVar,
    ConfigurableResource,
)

# Import all modules
from himalayan_etl import assets, jobs
from himalayan_etl.resources import (
    DatabaseResource,
    FileSystemResource,
    ETLConfigResource,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("etl.log")],
)

logger = logging.getLogger(__name__)


def create_dagster_definitions() -> Definitions:
    """
    Create Dagster definitions with all assets, jobs, schedules, and resources.
    """

    # Load all assets
    all_assets = load_assets_from_modules([assets])

    # Define resources with environment variable configuration
    resources = {
        "db": DatabaseResource(
            server=EnvVar("DB_SERVER"),
            database=EnvVar("DB_DATABASE"),
            username=EnvVar("DB_USERNAME"),
            password=EnvVar("DB_PASSWORD"),
            driver=EnvVar("DB_DRIVER"),
            pool_size=EnvVar.int("DB_POOL_SIZE"),
            pool_timeout=EnvVar.int("DB_POOL_TIMEOUT"),
        ),
        "fs": FileSystemResource(
            base_path=EnvVar("DATA_PATH"), encoding=EnvVar("FILE_ENCODING")
        ),
        "etl_config": ETLConfigResource(
            environment=EnvVar("ENVIRONMENT"),
            debug_mode=bool(EnvVar("DEBUG_MODE")),
            data_quality_threshold=0.95,
        ),
    }

    # Import jobs and schedules
    all_jobs = [
        jobs.himalayan_etl_full_load,
        jobs.himalayan_etl_incremental,
        jobs.world_bank_data_refresh,
        jobs.data_quality_assessment,
    ]

    all_schedules = [
        jobs.daily_incremental_schedule,
        jobs.monthly_world_bank_schedule,
        jobs.weekly_quality_assessment_schedule,
    ]

    all_sensors = [jobs.file_update_sensor]

    return Definitions(
        assets=all_assets,
        jobs=all_jobs,
        schedules=all_schedules,
        sensors=all_sensors,
        resources=resources,
    )


# Dagster definitions for discovery
# The 'defs' object is now imported from himalayan_etl.__init__.py
# This ensures that the CLI uses the same Definitions object as the Dagster UI/daemon
defs = create_dagster_definitions()


# CLI Interface
@click.group()
@click.option("--debug", is_flag=True, help="Enable debug mode")
@click.option("--config-file", help="Path to configuration file")
@click.pass_context
def cli(ctx, debug, config_file):
    """
    Himalayan Expeditions ETL Command Line Interface.

    This CLI provides commands to run ETL processes, validate data,
    and manage the data warehouse.
    """
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug
    ctx.obj["config_file"] = config_file

    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled")


@cli.command()
@click.option("--data-path", required=True, help="Path to CSV data files")
@click.option("--connection-string", help="Database connection string")
@click.option("--batch-size", default=10000, help="Batch size for database operations")
@click.option("--skip-world-bank", is_flag=True, help="Skip World Bank data extraction")
@click.pass_context
def run_full_etl(ctx, data_path, connection_string, batch_size, skip_world_bank):
    """
    Run the complete ETL process for initial data warehouse loading.
    """
    click.echo("🏔️  Starting Himalayan Expeditions ETL Process")
    click.echo(f"📁 Data path: {data_path}")

    try:
        # Validate data path
        data_path_obj = Path(data_path)
        if not data_path_obj.exists():
            click.echo(f"❌ Data path does not exist: {data_path}", err=True)
            sys.exit(1)

        # Check for required CSV files
        required_files = ["exped.csv", "members.csv", "peaks.csv", "refer.csv"]
        missing_files = []

        for filename in required_files:
            if not (data_path_obj / filename).exists():
                missing_files.append(filename)

        if missing_files:
            click.echo(
                f"❌ Missing required files: {', '.join(missing_files)}", err=True
            )
            sys.exit(1)

        # Set environment variables
        os.environ["DATA_PATH"] = str(data_path_obj)
        if connection_string:
            os.environ["DB_CONNECTION_STRING"] = connection_string

        click.echo("✅ Data validation passed")
        click.echo("🚀 Starting ETL process...")

        # Import and run the ETL process
        from dagster import DagsterInstance, execute_job

        instance = DagsterInstance.ephemeral()

        # Execute the full ETL job
        result = execute_job(
            jobs.himalayan_etl_full_load,
            instance=instance,
            run_config={
                "resources": {
                    "fs": {"config": {"base_path": str(data_path_obj)}},
                    "etl_config": {
                        "config": {
                            "load_world_bank_data": not skip_world_bank,
                            "batch_size": batch_size,
                        }
                    },
                }
            },
        )

        if result.success:
            click.echo("✅ ETL process completed successfully!")
            click.echo(f"📊 Run ID: {result.run_id}")
        else:
            click.echo("❌ ETL process failed!", err=True)
            for event in result.all_events:
                if event.is_failure:
                    click.echo(f"Error: {event.message}", err=True)
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Error running ETL: {str(e)}", err=True)
        if ctx.obj["debug"]:
            import traceback

            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option("--data-path", required=True, help="Path to CSV data files")
@click.pass_context
def validate_data(ctx, data_path):
    """
    Validate CSV data files for quality and completeness.
    """
    click.echo("🔍 Validating data files...")

    try:
        from himalayan_etl.resources import FileSystemResource

        fs = FileSystemResource(base_path=data_path)

        # Check each required file
        files_to_check = ["exped.csv", "members.csv", "peaks.csv", "refer.csv"]
        validation_results = {}

        for filename in files_to_check:
            click.echo(f"📋 Checking {filename}...")

            try:
                df = fs.read_csv_file(filename)

                # Basic validation metrics
                validation_results[filename] = {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "null_percentage": (
                        df.isnull().sum().sum() / (len(df) * len(df.columns))
                    )
                    * 100,
                    "duplicate_rows": df.duplicated().sum(),
                    "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
                }

                click.echo(f"  ✅ Rows: {validation_results[filename]['rows']:,}")
                click.echo(f"  ✅ Columns: {validation_results[filename]['columns']}")
                click.echo(
                    f"  ⚠️  Null values: {validation_results[filename]['null_percentage']:.1f}%"
                )
                click.echo(
                    f"  ⚠️  Duplicates: {validation_results[filename]['duplicate_rows']:,}"
                )

            except Exception as e:
                click.echo(f"  ❌ Error reading {filename}: {str(e)}", err=True)
                validation_results[filename] = {"error": str(e)}

        # Summary
        click.echo("\n📊 Validation Summary:")
        total_rows = sum([r.get("rows", 0) for r in validation_results.values()])
        total_files_ok = len(
            [r for r in validation_results.values() if "error" not in r]
        )

        click.echo(f"Files processed: {total_files_ok}/{len(files_to_check)}")
        click.echo(f"Total records: {total_rows:,}")

        if total_files_ok == len(files_to_check):
            click.echo("✅ All files passed validation!")
        else:
            click.echo("⚠️  Some files had issues - check output above")

    except Exception as e:
        click.echo(f"❌ Validation error: {str(e)}", err=True)
        if ctx.obj["debug"]:
            import traceback

            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option("--output-format", type=click.Choice(["json", "table"]), default="table")
@click.pass_context
def status(ctx, output_format):
    """
    Check the status of the data warehouse and recent ETL runs.
    """
    click.echo("📊 Data Warehouse Status")

    try:
        # This would connect to the database and check table sizes, last update times, etc.
        # For now, we'll show a mock status

        status_data = {
            "last_etl_run": "2024-01-15 02:00:00",
            "total_expeditions": 10500,
            "total_members": 85000,
            "data_quality_score": 0.92,
            "latest_expedition_year": 2023,
            "database_size_mb": 250.5,
        }

        if output_format == "json":
            import json

            click.echo(json.dumps(status_data, indent=2))
        else:
            click.echo(f"Last ETL Run: {status_data['last_etl_run']}")
            click.echo(f"Total Expeditions: {status_data['total_expeditions']:,}")
            click.echo(f"Total Members: {status_data['total_members']:,}")
            click.echo(f"Data Quality Score: {status_data['data_quality_score']:.2%}")
            click.echo(f"Latest Data Year: {status_data['latest_expedition_year']}")
            click.echo(f"Database Size: {status_data['database_size_mb']:.1f} MB")

    except Exception as e:
        click.echo(f"❌ Error checking status: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--port", default=3000, help="Port for Dagster UI")
@click.option("--host", default="localhost", help="Host for Dagster UI")
def serve(port, host):
    """
    Start the Dagster development server for monitoring and running jobs.
    """
    click.echo(f"🚀 Starting Dagster UI at http://{host}:{port}")
    click.echo("💡 Use this interface to monitor ETL jobs and data lineage")

    try:
        # Instead of DagsterInstance, we'll use dagster dev command
        # which is the standard way to run the UI.
        # This requires dagster-webserver to be installed.
        import subprocess

        # The command needs to point to the Definitions object.
        # Assuming this main.py is in himalayan_etl/main.py
        # and __init__.py is in himalayan_etl/__init__.py
        # The module is himalayan_etl and the attribute is defs
        cmd = [
            sys.executable,  # Use the current Python interpreter
            "-m",
            "dagster",
            "dev",
            "-m",
            "himalayan_etl",  # Python module where 'defs' is located
            "-a",
            "defs",  # Attribute name for Definitions object
            "-p",
            str(port),
            "-h",
            host,
        ]
        click.echo(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

    except FileNotFoundError:
        click.echo(
            "❌ 'dagster' command not found. Make sure Dagster is installed and in your PATH.",
            err=True,
        )
        click.echo("💡 Install with: pip install dagster dagster-webserver", err=True)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        click.echo(f"❌ Error starting Dagster UI: {e}", err=True)
        sys.exit(1)
    except ImportError:
        click.echo("❌ Dagster library not found. Please install it.", err=True)
        click.echo("💡 Install with: pip install dagster dagster-webserver", err=True)
        sys.exit(1)


@cli.command()
def init_db():
    """
    Initialize the database schema for the data warehouse.
    """
    click.echo("🗄️  Initializing database schema...")

    try:
        # Read and execute the SQL schema
        schema_path = Path(__file__).parent.parent.parent / "sql" / "create_schema.sql"

        if not schema_path.exists():
            click.echo(f"❌ Schema file not found: {schema_path}", err=True)
            sys.exit(1)

        click.echo(f"📄 Reading schema from: {schema_path}")

        with open(schema_path, "r") as f:
            schema_sql = f.read()

        # This would execute the SQL - for now just show what would happen
        click.echo("✅ Database schema would be created with:")
        click.echo("  - 8 dimension tables")
        click.echo("  - 1 fact table")
        click.echo("  - 1 bridge table")
        click.echo("  - ETL control tables")
        click.echo("  - Indexes and constraints")

        click.echo("💡 Connect to your database and run the SQL script manually")

    except Exception as e:
        click.echo(f"❌ Error initializing database: {str(e)}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
