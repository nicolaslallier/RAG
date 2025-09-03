# PostgreSQL Connection Test

This project contains a Python script to test the connection to a PostgreSQL database hosted on Azure Database for PostgreSQL.

## Features

- Tests PostgreSQL database connection
- Performs basic database operations (create table, insert, query, delete)
- Supports environment variables for database configuration
- Comprehensive logging and error handling
- Clean test data management

## Prerequisites

- Python 3.7 or higher
- Access to a PostgreSQL database (Azure Database for PostgreSQL in this case)

## Installation

1. Clone or download this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

### Option 1: Environment Variables (Recommended)

1. Copy the example environment file:
```bash
cp env.example .env
```

2. Edit the `.env` file with your actual database credentials:
```
DATABASE_URL=postgres://your_username:your_password@your_host:5432/your_database?sslmode=require
```

### Option 2: Direct Configuration

The script includes a default connection string that you can modify directly in the `test_db_connection.py` file.

## Usage

Run the connection test script:

```bash
python test_db_connection.py
```

### Expected Output

If the connection is successful, you should see output similar to:

```
2024-01-XX XX:XX:XX - INFO - Starting PostgreSQL connection test...
2024-01-XX XX:XX:XX - INFO - Attempting to connect to PostgreSQL database...
2024-01-XX XX:XX:XX - INFO - Connection string: postgres://***:***@mypgflex.postgres.database.azure.com:5432/postgres?sslmode=require
2024-01-XX XX:XX:XX - INFO - ✅ Successfully connected to PostgreSQL database!
2024-01-XX XX:XX:XX - INFO - Testing basic query...
2024-01-XX XX:XX:XX - INFO - PostgreSQL version: PostgreSQL 15.4 on x86_64-pc-linux-gnu...
2024-01-XX XX:XX:XX - INFO - Getting database information...
2024-01-XX XX:XX:XX - INFO - Database: postgres
2024-01-XX XX:XX:XX - INFO - User: mcppostgres
2024-01-XX XX:XX:XX - INFO - Server IP: XX.XX.XX.XX
2024-01-XX XX:XX:XX - INFO - Server Port: 5432
2024-01-XX XX:XX:XX - INFO - Testing table creation and data insertion...
2024-01-XX XX:XX:XX - INFO - ✅ Test data inserted: ID=1, Message='Connection test successful!', Created=2024-01-XX XX:XX:XX
2024-01-XX XX:XX:XX - INFO - Total test records: 1
2024-01-XX XX:XX:XX - INFO - Test table cleaned up
2024-01-XX XX:XX:XX - INFO - ✅ All database operations completed successfully!
2024-01-XX XX:XX:XX - INFO - Database connection closed
2024-01-XX XX:XX:XX - INFO - 🎉 Database connection test completed successfully!
```

## What the Script Tests

1. **Connection**: Establishes a connection to the PostgreSQL database
2. **Version Query**: Retrieves and displays the PostgreSQL version
3. **Database Info**: Shows current database, user, server IP, and port
4. **Table Operations**: Creates a test table, inserts data, queries it, and cleans up
5. **Transaction Management**: Properly commits transactions and closes connections

## Troubleshooting

### Common Issues

1. **Connection Refused**: Check if the database server is running and accessible
2. **Authentication Failed**: Verify username and password are correct
3. **SSL Connection Required**: Ensure `sslmode=require` is included in the connection string
4. **Firewall Issues**: Check if your IP is whitelisted in the Azure database firewall rules

### Error Messages

- `psycopg2.OperationalError`: Usually indicates connection issues (network, authentication, etc.)
- `psycopg2.ProgrammingError`: Usually indicates SQL syntax or permission issues
- `psycopg2.IntegrityError`: Usually indicates constraint violations

## Security Notes

- Never commit actual database credentials to version control
- Use environment variables for sensitive information
- The connection string in the script is for demonstration purposes only
- Consider using Azure Key Vault or similar services for production environments

## Dependencies

- `psycopg2-binary`: PostgreSQL adapter for Python
- `python-dotenv`: Load environment variables from .env files

## License

This project is provided as-is for testing and educational purposes.
