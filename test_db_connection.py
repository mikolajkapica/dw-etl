"""
Minimal MS SQL Server Connection Test

Simple script to test database connectivity.
"""

import os
import pyodbc
from dotenv import load_dotenv


def test_connection():
    """Test database connection with minimal setup."""
    # Load environment variables
    load_dotenv()

    print(f"envs = {os.environ}")
    
    # Get connection parameters
    server = os.getenv('DB_SERVER', 'localhost')
    database = os.getenv('DB_DATABASE', 'HimalayanExpeditionsDW')
    username = os.getenv('DB_USERNAME', '')
    password = os.getenv('DB_PASSWORD', '')
    
    # Build connection string
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;" # Use Windows Authentication
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    )
    
    try:
        print(f"Connecting to {server}/{database}...")
        
        # Test connection
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            
        if result[0] == 1:
            print("✅ Connection successful!")
            return True
        else:
            print("❌ Connection test failed")
            return False
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()
