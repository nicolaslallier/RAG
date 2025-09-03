#!/usr/bin/env python3
"""
PostgreSQL Database Connection Test Script

This script tests the connection to a PostgreSQL database and performs basic operations
to verify the connection is working properly.
"""

import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_database_config():
    """Load database configuration from environment variables or use defaults."""
    load_dotenv()
    
    # Default connection string (you can override with environment variables)
    default_connection_string = "postgres://mcppostgres:SuperSecret123@mypgflex.postgres.database.azure.com:5432/postgres?sslmode=require"
    
    # Try to get from environment variable first, then use default
    connection_string = os.getenv('DATABASE_URL', default_connection_string)
    
    return connection_string

def test_connection():
    """Test the PostgreSQL database connection."""
    connection_string = load_database_config()
    
    try:
        logger.info("Attempting to connect to PostgreSQL database...")
        logger.info(f"Connection string: {connection_string.replace(connection_string.split('@')[0].split('//')[1], '***:***')}")
        
        # Establish connection
        conn = psycopg2.connect(connection_string)
        logger.info("✅ Successfully connected to PostgreSQL database!")
        
        # Create a cursor
        cursor = conn.cursor()
        
        # Test basic query
        logger.info("Testing basic query...")
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info(f"PostgreSQL version: {version[0]}")
        
        # Test database info
        logger.info("Getting database information...")
        cursor.execute("SELECT current_database(), current_user, inet_server_addr(), inet_server_port();")
        db_info = cursor.fetchone()
        logger.info(f"Database: {db_info[0]}")
        logger.info(f"User: {db_info[1]}")
        logger.info(f"Server IP: {db_info[2]}")
        logger.info(f"Server Port: {db_info[3]}")
        
        # Test creating a simple table and inserting data
        logger.info("Testing table creation and data insertion...")
        
        # Create a test table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS connection_test (
                id SERIAL PRIMARY KEY,
                test_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Insert test data
        cursor.execute("""
            INSERT INTO connection_test (test_message) 
            VALUES (%s) 
            RETURNING id, test_message, created_at;
        """, ("Connection test successful!",))
        
        result = cursor.fetchone()
        logger.info(f"✅ Test data inserted: ID={result[0]}, Message='{result[1]}', Created={result[2]}")
        
        # Query the test data
        cursor.execute("SELECT COUNT(*) FROM connection_test;")
        count = cursor.fetchone()[0]
        logger.info(f"Total test records: {count}")
        
        # Clean up test table (optional - comment out if you want to keep the table)
        cursor.execute("DROP TABLE IF EXISTS connection_test;")
        logger.info("Test table cleaned up")
        
        # Commit the transaction
        conn.commit()
        logger.info("✅ All database operations completed successfully!")
        
        return True
        
    except psycopg2.Error as e:
        logger.error(f"❌ PostgreSQL error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False
    finally:
        # Close the connection
        if 'conn' in locals():
            cursor.close()
            conn.close()
            logger.info("Database connection closed")

def main():
    """Main function to run the connection test."""
    logger.info("Starting PostgreSQL connection test...")
    
    success = test_connection()
    
    if success:
        logger.info("🎉 Database connection test completed successfully!")
        sys.exit(0)
    else:
        logger.error("💥 Database connection test failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
