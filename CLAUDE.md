# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AWS Lambda-based data synchronization system that transfers data from SQL Server databases to PostgreSQL. Supports multiple source databases (McTM and VoipDB) with batch processing, sequential execution, and comprehensive error handling.

## Core Architecture

### Main Components

- **lambda_function.py** - AWS Lambda entry point with CLI support and multiple execution modes
- **multi_table_manager.py** - Orchestrates batch synchronization with progress tracking
- **table_sync_processor.py** - Handles individual table sync with batched insertions
- **table_configs.py** - Centralized table definitions with column mappings
- **config.py** - Environment variable management with validation

### Data Flow Architecture

1. **Source Databases**: Two SQL Server instances with different data types
   - McTM (10.35.11.13) - Customer and module management data
   - VoipDB (10.35.11.14) - VoIP infrastructure and user agent data

2. **Processing Pipeline**: Sequential table processing with batched operations
   - Data extraction with optimized SELECT queries using NOLOCK
   - Batch insertion to PostgreSQL using execute_values for performance
   - Transaction-based operations with rollback on failure

3. **Target Database**: PostgreSQL with normalized naming (e.g., mctm_customer, voipdb_useragent)

### Execution Patterns

- **Sequential Processing**: Tables processed one at a time to avoid resource conflicts
- **Batched Operations**: Configurable batch sizes (default 10,000) for memory efficiency
- **Full Table Sync**: TRUNCATE + INSERT pattern for data consistency

## Development Commands

### Environment Setup
```bash
# Copy and configure environment variables
cp .env.example .env
# Edit .env with actual database credentials

# Validate configuration
python lambda_deployment_postgresql_updated/config.py
```

### Local Development
```bash
cd lambda_deployment_postgresql_updated

# Multi-table sync (default mode)
python lambda_function.py multi_sync

# Connection testing for all databases
python lambda_function.py multi_test

# Single table operations
python lambda_function.py single_sync --table=customer
python lambda_function.py single_sync --table=mctm_module

# Custom table selection
python lambda_function.py multi_sync --tables=customer,voipdb_customer

# Get table information and metadata
python lambda_function.py info
```

### Direct Component Testing
```bash
# Test individual table processor
python table_sync_processor.py customer sync
python table_sync_processor.py customer test

# Test multi-table manager
python multi_table_manager.py sync
python multi_table_manager.py test --tables=customer,mctm_module
```

### Lambda Deployment
```bash
cd lambda_deployment_postgresql_updated
./build.sh
# Creates lambda_function_with_postgresql.zip for AWS deployment
```

## Configuration Management

### Environment Variables Structure
**McTM Database:**
```
SQL_SERVER_MCTM_HOST=10.35.11.13
SQL_SERVER_MCTM_DB=McTM
SQL_SERVER_MCTM_USER=sa
SQL_SERVER_MCTM_PASSWORD=password
SQL_SERVER_MCTM_PORT=1433
```

**VoipDB Database:**
```
SQL_SERVER_VOIPDB_HOST=10.35.11.14
SQL_SERVER_VOIPDB_DB=VoipDB
SQL_SERVER_VOIPDB_USER=sa
SQL_SERVER_VOIPDB_PASSWORD=password
SQL_SERVER_VOIPDB_PORT=1433
```

**PostgreSQL:**
```
PG_HOST=your-rds-endpoint.amazonaws.com
PG_DB=dx_sim_management_db
PG_USER=postgres
PG_PASSWORD=password
PG_PORT=5432
```

### Adding New Tables
1. **table_configs.py**: Add entry to `TABLE_CONFIGS` dictionary
2. **Required fields**: `db_type`, `sql_table`, `pg_table`, `columns`, `pg_columns`, `primary_key`, `order_by`, `batch_size`
3. **Column mapping**: Ensure SQL Server columns match PostgreSQL columns (case conversion)
4. **DEFAULT_SYNC_ORDER**: Add table to sync sequence if needed

### Table Configuration Pattern
```python
'table_name': {
    'db_type': 'mctm' | 'voipdb',
    'sql_server_db': 'McTM' | 'VoipDB',
    'sql_table': '[Database].[dbo].[TableName]',
    'pg_table': 'normalized_table_name',
    'columns': ['Col1', 'Col2'],          # SQL Server columns
    'pg_columns': ['col1', 'col2'],       # PostgreSQL columns (lowercase)
    'primary_key': 'ID',
    'order_by': 'ID',
    'batch_size': 10000,
    'description': 'Human readable description'
}
```

## Available Tables

Current synchronized tables:
- **customer** - McTM customer master data
- **mctm_module** - McTM module management  
- **voipdb_customer** - VoipDB customer information
- **voipdb_useragent** - VoipDB user agent data

## Lambda Execution Modes

- **multi_sync** - Sequential sync of multiple tables with progress tracking
- **multi_test** - Connection validation for all configured databases
- **single_sync** - Individual table sync (requires table_name parameter)  
- **info** - Returns table metadata and configuration details

## Dependencies

### Runtime Dependencies
- **pymssql** - SQL Server connectivity with connection pooling
- **psycopg2** - PostgreSQL connectivity with batch operations
- **boto3** - AWS Lambda runtime support

### Development Dependencies  
- **python-dotenv** - Environment variable loading for local development