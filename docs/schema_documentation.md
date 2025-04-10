# Crypto Data Platform Schema Documentation

## Bronze Layer

The bronze layer contains raw data loaded from JSON files, maintaining the original structure with minimal transformations. Each table includes metadata about when and how the data was loaded.

### bronze_assets
- **affiliate_id** (VARCHAR(50)): Identifier for the affiliate
- **asset_id** (VARCHAR(50)): Unique identifier for the asset
- **symbol** (VARCHAR(50)): Trading symbol
- **name** (VARCHAR(100)): Asset name
- **data** (JSONB): Raw JSON data containing additional asset information
- **load_timestamp** (TIMESTAMP): When this record was loaded into the database
- **api_run_timestamp** (TIMESTAMP): When the API call that fetched this data was made
- **source_file** (VARCHAR(255)): Path to the source JSON file
- **load_status** (VARCHAR(20)): Status of the load (SUCCESS, ERROR, PARTIAL)
- **error_message** (TEXT): Any error message if load_status is ERROR

### bronze_customers
- **affiliate_id** (VARCHAR(50)): Identifier for the affiliate
- **customer_id** (VARCHAR(50)): Unique identifier for the customer
- **data** (JSONB): Raw JSON data containing customer information
- **load_timestamp** (TIMESTAMP): When this record was loaded into the database
- **api_run_timestamp** (TIMESTAMP): When the API call that fetched this data was made
- **source_file** (VARCHAR(255)): Path to the source JSON file
- **load_status** (VARCHAR(20)): Status of the load (SUCCESS, ERROR, PARTIAL)
- **error_message** (TEXT): Any error message if load_status is ERROR

### bronze_deposits
- **affiliate_id** (VARCHAR(50)): Identifier for the affiliate
- **deposit_id** (VARCHAR(50)): Unique identifier for the deposit
- **data** (JSONB): Raw JSON data containing deposit information
- **load_timestamp** (TIMESTAMP): When this record was loaded into the database
- **api_run_timestamp** (TIMESTAMP): When the API call that fetched this data was made
- **source_file** (VARCHAR(255)): Path to the source JSON file
- **load_status** (VARCHAR(20)): Status of the load (SUCCESS, ERROR, PARTIAL)
- **error_message** (TEXT): Any error message if load_status is ERROR

### bronze_trades
- **affiliate_id** (VARCHAR(50)): Identifier for the affiliate
- **trade_id** (VARCHAR(50)): Unique identifier for the trade
- **data** (JSONB): Raw JSON data containing trade information
- **load_timestamp** (TIMESTAMP): When this record was loaded into the database
- **api_run_timestamp** (TIMESTAMP): When the API call that fetched this data was made
- **source_file** (VARCHAR(255)): Path to the source JSON file
- **load_status** (VARCHAR(20)): Status of the load (SUCCESS, ERROR, PARTIAL)
- **error_message** (TEXT): Any error message if load_status is ERROR

## Silver Layer

The silver layer contains cleaned and standardized data, ready for analysis.

### AffiliateAccount
- **affiliate_id** (STRING): Primary key, unique identifier for the affiliate
- **name** (STRING): Affiliate name
- **email** (STRING): Contact email
- **join_date** (TIMESTAMP): Date the affiliate joined
- **metadata** (JSON): Additional affiliate information

### ClientAccount
- **client_id** (STRING): Primary key, unique identifier for the client
- **affiliate_id** (STRING): Foreign key to AffiliateAccount
- **register_time** (TIMESTAMP): Client registration time
- **country** (STRING): Client's country
- **metadata** (JSON): Additional client information

### Deposits
- **deposit_id** (STRING): Primary key, unique identifier for the deposit
- **client_id** (STRING): Foreign key to ClientAccount
- **deposit_time** (TIMESTAMP): Deposit timestamp
- **deposit_coin** (STRING): Coin type (e.g., USDT)
- **deposit_amount** (FLOAT64): Amount deposited

### TradeActivities
- **trade_activity_id** (STRING): Primary key, unique identifier for the trade
- **client_id** (STRING): Foreign key to ClientAccount
- **trade_time** (TIMESTAMP): Trade timestamp
- **symbol** (STRING): Symbol traded
- **trade_volume** (FLOAT64): Volume traded

### Assets
- **asset_id** (STRING): Primary key, unique identifier for the asset
- **client_id** (STRING): Foreign key to ClientAccount
- **balance** (FLOAT64): Account balance
- **last_update_time** (TIMESTAMP): Last update time
- **remark** (STRING): Additional remarks

## Gold Layer

The gold layer contains aggregated views focused on affiliate performance metrics.

### gold_affiliate_daily_metrics
- **date** (DATE): Date of the metrics
- **affiliate_id** (STRING): Identifier for the affiliate
- **new_signups** (INTEGER): Number of new signups for the day
- **total_trades** (INTEGER): Total number of trades for the day
- **total_volume** (FLOAT64): Total trading volume for the day

### gold_affiliate_weekly_metrics
- **week_start** (DATE): Start of the week
- **affiliate_id** (STRING): Identifier for the affiliate
- **new_signups** (INTEGER): Number of new signups for the week
- **total_trades** (INTEGER): Total number of trades for the week
- **total_volume** (FLOAT64): Total trading volume for the week

### gold_affiliate_monthly_metrics
- **month_start** (DATE): Start of the month
- **affiliate_id** (STRING): Identifier for the affiliate
- **new_signups** (INTEGER): Number of new signups for the month
- **total_trades** (INTEGER): Total number of trades for the month
- **total_volume** (FLOAT64): Total trading volume for the month

### gold_affiliate_customer_funnel
- **affiliate_id** (STRING): Identifier for the affiliate
- **total_customers** (INTEGER): Total number of customers
- **new_customers_30d** (INTEGER): New customers in last 30 days
- **active_customers_30d** (INTEGER): Active customers in last 30 days
- **trades_30d** (INTEGER): Number of trades in last 30 days
- **volume_30d** (FLOAT64): Trading volume in last 30 days

### gold_affiliate_performance_trend
- **date** (DATE): Date of the metrics
- **affiliate_id** (STRING): Identifier for the affiliate
- **daily_trades** (INTEGER): Number of trades for the day
- **daily_volume** (FLOAT64): Trading volume for the day
- **weekly_trades** (INTEGER): Number of trades for the week
- **weekly_volume** (FLOAT64): Trading volume for the week
- **monthly_trades** (INTEGER): Number of trades for the month
- **monthly_volume** (FLOAT64): Trading volume for the month 