# MS SQL Server Connection Test

This standalone application helps you test your MS SQL Server database connection independently of the main ETL pipeline.

## Quick Start

### Option 1: Interactive Mode (Recommended)
```bash
python test_db_connection.py
```

### Option 2: Using Batch File (Windows)
```bash
test_db.bat
```

### Option 3: Using PowerShell (Windows)
```powershell
.\test_db.ps1
```

## Configuration

The application will automatically load database settings from your `.env` file, but you can also:

1. **Use Environment Variables**: The app reads from `.env` file in the current directory
2. **Interactive Input**: The app will prompt you for connection details
3. **Command Line**: Pass connection details as arguments

### Environment Variables
Copy `.env.db.template` to `.env` and update with your database credentials:

```bash
cp .env.db.template .env
# Edit .env with your database details
```

### Command Line Usage
```bash
# Basic connection test
python test_db_connection.py --server localhost --database HimalayanExpeditionsDW --username sa --password YourPassword

# Test only (no interactive mode)
python test_db_connection.py --server localhost --database testdb --username sa --password pass123 --test-only
```

## What This Tool Tests

1. **ODBC Driver Availability**: Lists all installed SQL Server ODBC drivers
2. **Database Connection**: Tests basic connectivity to your SQL Server instance
3. **Authentication**: Verifies your credentials work correctly
4. **Basic Queries**: Runs simple SELECT statements to test read access
5. **Write Operations**: Creates a test table to verify write permissions
6. **Database Schema**: Lists existing tables and checks for ETL tables

## Common Issues and Solutions

### 1. "No ODBC drivers found"
**Solution**: Install Microsoft ODBC Driver for SQL Server
- Download from: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
- Install "ODBC Driver 17 for SQL Server" (recommended)

### 2. "Connection failed" with timeout
**Solutions**:
- Check if SQL Server is running: `services.msc` → SQL Server service
- Verify SQL Server allows remote connections
- Check Windows Firewall settings (port 1433)
- Ensure TCP/IP protocol is enabled in SQL Server Configuration Manager

### 3. "Login failed for user"
**Solutions**:
- Verify username and password are correct
- Check if SQL Server Authentication is enabled (not just Windows Authentication)
- Ensure the user account exists and has proper permissions
- For Windows Authentication, try running as administrator

### 4. "Database does not exist"
**Solutions**:
- Create the database first: `CREATE DATABASE HimalayanExpeditionsDW`
- Verify you have permission to access the specified database
- Check the database name spelling

### 5. "SSL/TLS connection issues"
**Solutions**:
- The connection string includes `TrustServerCertificate=yes` for testing
- For production, configure proper SSL certificates

## Interactive Query Mode

After successful connection, you can run SQL queries interactively:

```sql
-- Check server version
SELECT @@VERSION

-- List all databases
SELECT name FROM sys.databases

-- Check current database
SELECT DB_NAME()

-- List tables in current database
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'

-- Exit interactive mode
quit
```

## Driver Information

The tool will show you which ODBC drivers are available on your system. Common drivers include:

- **ODBC Driver 17 for SQL Server** (recommended, latest)
- **ODBC Driver 13 for SQL Server** (older but stable)
- **SQL Server Native Client 11.0** (legacy)
- **SQL Server** (very old, not recommended)

## Requirements

- Python 3.8+
- pyodbc
- sqlalchemy
- pandas
- python-dotenv

These will be automatically installed when you run the batch/PowerShell scripts.

## Troubleshooting Tips

1. **Run as Administrator**: Some ODBC operations may require elevated privileges
2. **Check SQL Server Configuration**: Ensure SQL Server Browser service is running for named instances
3. **Network Connectivity**: Use `telnet server_ip 1433` to test basic network connectivity
4. **Connection String**: The tool shows you the exact connection string being used
5. **Logs**: Check the console output for detailed error messages

## Example Output

```
🔗 MS SQL Server Connection Test Application
================================================

Loaded configuration:
  Server: localhost
  Database: HimalayanExpeditionsDW
  Username: sa
  Driver: ODBC Driver 17 for SQL Server

📋 Available ODBC Drivers:
['ODBC Driver 17 for SQL Server', 'SQL Server']

🔌 Connecting to database...
✅ Database connection successful!

🧪 Running database operation tests...
✅ Basic database tests completed successfully!

📝 Testing write operations...
✅ Test table created, populated, and cleaned up successfully!

✅ All tests passed! Database connection is working correctly.
```

This tool is designed to be run independently of your main ETL pipeline to quickly diagnose database connectivity issues.
