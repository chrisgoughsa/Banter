# Database Schema Documentation

## Overview

The database is structured in a multi-layer architecture following the medallion pattern (Bronze → Silver → Gold) to handle data ingestion, processing, and analytics. Each layer serves a specific purpose in the data pipeline.

## Layer Architecture

### 1. Base Schema (Core Tables)
The base schema contains the core tables that store the primary business entities.

#### AffiliateAccount
- Stores affiliate information
- Primary key: `affiliate_id`
- Fields:
  - `name` (STRING, NOT NULL): Affiliate name
  - `email` (STRING, NOT NULL): Contact email
  - `join_date` (TIMESTAMP, NOT NULL): Date the affiliate joined
  - `status` (STRING, NOT NULL, DEFAULT 'active'): Status of the affiliate
  - `metadata` (JSON): Optional extra information
  - Audit fields: `created_at`, `updated_at`, `created_by`, `updated_by`
- Indexes:
  - `idx_affiliate_status`: For status-based queries

#### ClientAccount
- Stores client information
- Primary key: `client_id`
- Foreign key: `affiliate_id` references AffiliateAccount
- Fields:
  - `register_time` (TIMESTAMP, NOT NULL): Client registration time
  - `register_date` (DATE, NOT NULL): Date portion of register_time
  - `country` (STRING): Client's country
  - `status` (STRING, NOT NULL, DEFAULT 'active'): Status of the client
  - `metadata` (JSON): Optional extra fields
  - Audit fields: `created_at`, `updated_at`, `created_by`, `updated_by`
- Indexes:
  - `idx_client_affiliate`: For affiliate-based queries
  - `idx_client_register_date`: For date-based queries
  - `idx_client_status`: For status-based queries

#### Deposits
- Stores deposit information
- Primary key: `deposit_id`
- Foreign key: `client_id` references ClientAccount
- Fields:
  - `deposit_time` (TIMESTAMP, NOT NULL): Deposit timestamp
  - `deposit_date` (DATE, NOT NULL): Date portion of deposit_time
  - `deposit_coin` (STRING, NOT NULL): Coin type (e.g., USDT)
  - `deposit_amount` (DECIMAL(18,8), NOT NULL): Amount deposited
  - `status` (STRING, NOT NULL, DEFAULT 'pending'): Status of the deposit
  - `metadata` (JSON): Optional extra fields
  - Audit fields: `created_at`, `updated_at`, `created_by`, `updated_by`
- Indexes:
  - `idx_deposit_client`: For client-based queries
  - `idx_deposit_date`: For date-based queries
  - `idx_deposit_status`: For status-based queries

#### TradeActivities
- Stores trade information
- Primary key: `trade_activity_id`
- Foreign key: `client_id` references ClientAccount
- Fields:
  - `trade_time` (TIMESTAMP, NOT NULL): Trade timestamp
  - `trade_date` (DATE, NOT NULL): Date portion of trade_time
  - `symbol` (STRING, NOT NULL): Symbol traded
  - `trade_volume` (DECIMAL(18,8), NOT NULL): Volume traded
  - `trade_type` (STRING, NOT NULL): Type of trade (buy/sell)
  - `status` (STRING, NOT NULL, DEFAULT 'completed'): Status of the trade
  - `metadata` (JSON): Optional extra fields
  - Audit fields: `created_at`, `updated_at`, `created_by`, `updated_by`
- Indexes:
  - `idx_trade_client`: For client-based queries
  - `idx_trade_date`: For date-based queries
  - `idx_trade_symbol`: For symbol-based queries
  - `idx_trade_status`: For status-based queries

#### Assets
- Stores asset information
- Primary key: `asset_id`
- Foreign key: `client_id` references ClientAccount
- Fields:
  - `balance` (DECIMAL(18,8), NOT NULL): Account balance
  - `last_update_time` (TIMESTAMP, NOT NULL): Last update time
  - `last_update_date` (DATE, NOT NULL): Date portion of last_update_time
  - `symbol` (STRING, NOT NULL): Asset symbol
  - `status` (STRING, NOT NULL, DEFAULT 'active'): Status of the asset
  - `remark` (STRING): Remarks
  - `metadata` (JSON): Optional extra fields
  - Audit fields: `created_at`, `updated_at`, `created_by`, `updated_by`
- Indexes:
  - `idx_asset_client`: For client-based queries
  - `idx_asset_update_date`: For date-based queries
  - `idx_asset_symbol`: For symbol-based queries
  - `idx_asset_status`: For status-based queries

#### AuditLog
- Tracks changes to all tables
- Primary key: `audit_id`
- Fields:
  - `table_name` (STRING, NOT NULL): Name of the table changed
  - `record_id` (STRING, NOT NULL): ID of the record changed
  - `action` (STRING, NOT NULL): Type of change (INSERT/UPDATE/DELETE)
  - `old_values` (JSON): Previous values
  - `new_values` (JSON): New values
  - `changed_by` (STRING, NOT NULL): User who made the change
  - `changed_at` (TIMESTAMP, NOT NULL): When the change was made
- Indexes:
  - `idx_audit_table_record`: For table and record lookups
  - `idx_audit_changed_at`: For temporal queries

### 2. Bronze Layer
The bronze layer handles raw data ingestion and initial storage.

#### bronze_assets, bronze_customers, bronze_deposits, bronze_trades
- Store raw data from various sources
- Common structure across all tables:
  - Primary key: Composite of `affiliate_id`, record ID, and `load_timestamp`
  - Fields:
    - `data` (JSONB, NOT NULL): Raw JSON data
    - `load_timestamp` (TIMESTAMP, NOT NULL): When the record was loaded
    - `api_run_timestamp` (TIMESTAMP, NOT NULL): When the API call was made
    - `source_file` (VARCHAR(255), NOT NULL): Source file path
    - `load_status` (VARCHAR(20), NOT NULL): Status of the load
    - `error_message` (TEXT): Error details if any
    - `retry_count` (INTEGER, DEFAULT 0): Number of retry attempts
    - `last_retry_timestamp` (TIMESTAMP): Last retry time
    - Audit fields: `created_at`, `updated_at`
- Partitioning:
  - Partitioned by `load_timestamp`
  - Three partitions: previous month, current month, next month
- Indexes:
  - `idx_bronze_*_load_status`: For status-based queries
  - `idx_bronze_*_api_run`: For API run time queries

#### etl_status
- Tracks ETL process status
- Primary key: `table_name`
- Fields:
  - `last_successful_load` (TIMESTAMP): Last successful load time
  - `last_attempted_load` (TIMESTAMP): Last attempted load time
  - `load_status` (VARCHAR(20), NOT NULL): Current status
  - `error_message` (TEXT): Error details if any
  - `records_processed` (INTEGER): Number of records processed
  - `processing_time` (INTERVAL): Time taken for processing
  - Audit fields: `created_at`, `updated_at`
- Indexes:
  - `idx_etl_status_load_status`: For status-based queries

### 3. Silver Layer
The silver layer handles data processing and transformation.

#### silver_customers, silver_deposits, silver_trades, silver_assets
- Store processed and validated data
- Common structure across all tables:
  - Primary key: Varies by table
  - Fields:
    - Business-specific fields (e.g., `register_time`, `deposit_amount`)
    - `status` (VARCHAR(20), NOT NULL): Status of the record
    - `metadata` (JSONB): Additional information
    - `source_file` (TEXT, NOT NULL): Source file path
    - `load_time` (TIMESTAMP, NOT NULL): Load time
    - Audit fields: `created_at`, `updated_at`
- Partitioning:
  - Partitioned by date fields (e.g., `register_date`, `deposit_date`)
  - Two partitions: previous year and current year
- Indexes:
  - Various indexes for common query patterns
  - Status-based indexes
  - Date-based indexes

#### data_quality_metrics
- Tracks data quality metrics
- Primary key: Composite of `table_name` and `metric_date`
- Fields:
  - `total_records` (BIGINT, NOT NULL): Total records
  - `valid_records` (BIGINT, NOT NULL): Valid records
  - `invalid_records` (BIGINT, NOT NULL): Invalid records
  - Quality scores (DECIMAL(5,2)): Completeness, accuracy, consistency, timeliness
  - Audit fields: `created_at`, `updated_at`
- Indexes:
  - `idx_data_quality_metric_date`: For temporal queries

### 4. Gold Layer
The gold layer provides analytics and reporting capabilities.

#### Materialized Views
1. **gold_affiliate_daily_metrics**
   - Daily metrics for affiliates
   - Includes signups, trades, deposits
   - Indexed by date and affiliate_id

2. **gold_affiliate_weekly_metrics**
   - Weekly metrics for affiliates
   - Similar structure to daily metrics
   - Indexed by week_start and affiliate_id

3. **gold_affiliate_monthly_metrics**
   - Monthly metrics for affiliates
   - Similar structure to daily metrics
   - Indexed by month_start and affiliate_id

4. **gold_affiliate_customer_funnel**
   - Customer funnel metrics
   - Tracks customer progression
   - Indexed by affiliate_id

5. **gold_affiliate_performance_trend**
   - Performance trends over time
   - Includes daily, weekly, and monthly metrics
   - Indexed by date and affiliate_id

6. **gold_etl_affiliate_dashboard**
   - Combined ETL and affiliate performance metrics
   - Indexed by affiliate_id and data_source

#### Refresh Function
- `refresh_all_materialized_views()`: Refreshes all materialized views concurrently

## Data Flow

1. **Ingestion (Bronze)**
   - Raw data is ingested from various sources
   - Data is stored with minimal transformation
   - ETL status is tracked
   - Retry mechanism for failed loads

2. **Processing (Silver)**
   - Data is validated and transformed
   - Business rules are applied
   - Data quality is monitored
   - Deduplication is performed

3. **Analytics (Gold)**
   - Materialized views provide aggregated metrics
   - Performance is optimized with indexes
   - Views are refreshed periodically
   - Comprehensive analytics are available

## Best Practices

1. **Data Integrity**
   - Use NOT NULL constraints where appropriate
   - Implement foreign key relationships
   - Validate data at insertion
   - Track changes in AuditLog

2. **Performance**
   - Use appropriate indexes
   - Implement table partitioning
   - Use materialized views for complex queries
   - Monitor query performance

3. **Maintenance**
   - Regular view refreshes
   - Monitor data quality
   - Track ETL processes
   - Maintain audit trails

4. **Security**
   - Implement row-level security
   - Use appropriate access controls
   - Audit sensitive operations
   - Protect sensitive data

## Future Considerations

1. **Scalability**
   - Consider sharding for large tables
   - Implement read replicas
   - Optimize partition strategies

2. **Monitoring**
   - Implement comprehensive monitoring
   - Set up alerts for issues
   - Track performance metrics

3. **Data Retention**
   - Implement data archiving
   - Define retention policies
   - Consider cold storage options 