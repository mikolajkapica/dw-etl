# Himalayan Expeditions Data Warehouse ETL

This project implements a comprehensive ETL pipeline for analyzing historical Himalayan expeditions data using the Dagster framework and MS SQL Server as the target data warehouse.

## Project Overview

The ETL pipeline processes:
- Himalayan Database (expeditions, members, peaks, references)
- World Bank Development Indicators
- Transforms and loads data into a dimensional data warehouse

## Architecture

```
Data Sources → Extract → Transform → Load → Data Warehouse
     ↓           ↓         ↓        ↓           ↓
Himalayan DB    CSV      Clean     SQL      Dimensional
World Bank  →  Files  →  Data   → Server →   Schema
```

## Quick Start

1. **Setup Environment**
   ```bash
   # Clone and navigate to project
   cd dw-etl
   
   # Install dependencies
   pip install -e .
   
   # Copy and configure environment
   cp .env.example .env
   # Edit .env with your database credentials
   ```

2. **Database Setup**
   ```bash
   # Create database schema
   sqlcmd -S localhost -d master -i sql/create_schema.sql
   ```

3. **Prepare Data**
   ```bash
   # Place your CSV files in data/ directory:
   # - data/exped.csv
   # - data/members.csv
   # - data/peaks.csv
   # - data/refer.csv
   # - data/wdi.csv
   ```

4. **Run ETL Pipeline**
   ```bash
   # Start Dagster development server
   dagster dev
   
   # Access web interface at http://localhost:3000
   ```

## Data Sources

### Himalayan Database
- **exped.csv**: Expedition information and outcomes
- **members.csv**: Individual expedition members
- **peaks.csv**: Mountain peaks and their characteristics
- **refer.csv**: Literature references

### World Bank Indicators
- **wdi.csv**: Economic and development indicators by country/year

## Data Warehouse Schema

### Dimension Tables
- `DIM_Date`: Date dimension for time-based analysis
- `DIM_Nationality`: Country/nationality information
- `DIM_Peak`: Mountain peaks and characteristics
- `DIM_Route`: Climbing routes and difficulty
- `DIM_ExpeditionStatus`: Status codes and termination reasons
- `DIM_HostCountry`: Host countries for expeditions
- `DIM_Member`: Expedition members (SCD Type 2)
- `DIM_CountryIndicators`: World Bank economic indicators

### Fact Tables
- `FACT_Expeditions`: Main fact table with expedition metrics
- `BRIDGE_ExpeditionMembers`: Many-to-many relationship bridge

## Pipeline Structure

The ETL pipeline is organized into the following Dagster jobs:

1. **Data Extraction Jobs**
   - Extract Himalayan data from CSV files
   - Extract World Bank indicators

2. **Data Cleaning Jobs**
   - Handle missing values and data type conversions
   - Standardize codes and text fields
   - Remove duplicates and inconsistencies

3. **Dimension Loading Jobs**
   - Process and load dimension tables
   - Handle slowly changing dimensions

4. **Fact Loading Jobs**
   - Join and aggregate data for fact tables
   - Load expedition facts and member relationships

## Development

### Project Structure
```
dw-etl/
├── src/himalayan_etl/
│   ├── __init__.py
│   ├── resources/          # Database and file system resources
│   ├── ops/               # Individual ETL operations
│   ├── jobs/              # Pipeline job definitions
│   ├── assets/            # Data assets and dependencies
│   └── utils/             # Utility functions
├── sql/                   # SQL DDL scripts
├── data/                  # Source data files
├── tests/                 # Unit and integration tests
└── config/                # Configuration files
```

### Running Tests
```bash
pytest tests/
```

### Code Quality
```bash
# Format code
black src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/
```

## Monitoring and Operations

### Dagster Web Interface
- Pipeline execution monitoring
- Data lineage visualization
- Asset materialization tracking
- Error investigation and retry

### ETL Audit Trail
- All operations logged to `ETL_AuditLog` table
- Configurable retry policies
- Data quality validation

## Configuration

Key configuration options in `.env`:
- Database connection parameters
- Data source file paths
- Batch processing sizes
- Retry policies and logging levels

## Troubleshooting

### Common Issues
1. **Database Connection**: Verify SQL Server credentials and network access
2. **Missing Data Files**: Ensure CSV files are in the correct data/ directory
3. **Memory Issues**: Adjust batch sizes for large datasets
4. **Data Quality**: Check ETL audit logs for data validation errors

### Support
- Check Dagster logs in the web interface
- Review `ETL_AuditLog` table for detailed error information
- Consult data quality reports in pipeline outputs
