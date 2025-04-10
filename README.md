# Crypto Data Platform

A data platform for processing and analyzing cryptocurrency trading data across multiple affiliates.

## Project Structure

```
crypto_data_platform/
├── src/                      # Source code
│   ├── etl/                  # ETL processing logic
│   │   ├── bronze/          # Raw data extraction and loading
│   │   ├── silver/          # Data transformation and cleaning
│   │   └── gold/            # Analytical views
│   ├── config/              # Configuration management
│   ├── utils/               # Shared utilities
│   ├── dashboard/           # Dashboard application
│   └── scripts/             # Utility scripts
├── tests/                   # Test files
├── data/                    # Data directory
│   ├── bronze/             # Raw data files
│   ├── silver/             # Transformed data
│   └── gold/               # Analytical views
├── alembic/                # Database migrations
├── main.py                 # Main entry point
├── pyproject.toml          # Project dependencies
└── README.md               # This file
```

## Quick Start

1. Install dependencies:
   ```bash
   poetry install
   ```

2. Set up the database:
   ```bash
   poetry run python src/scripts/clear_database.py  # If needed
   ```

3. Run the ETL pipeline:
   ```bash
   # Run the complete pipeline
   poetry run python main.py

   # Or run specific layers
   poetry run python main.py --layer bronze --days 7
   poetry run python main.py --layer silver
   poetry run python main.py --layer gold
   ```

4. Start the dashboard:
   ```bash
   poetry run python src/dashboard/app/main.py
   ```

## ETL Pipeline

The platform uses a three-layer ETL architecture:

1. **Bronze Layer**: Raw data extraction and loading
   - Extracts data from JSON files
   - Performs basic validation
   - Loads into bronze tables

2. **Silver Layer**: Data transformation and cleaning
   - Cleans and standardizes data
   - Enforces data quality rules
   - Creates normalized tables with relationships

3. **Gold Layer**: Analytics and reporting
   - Creates analytical views
   - Calculates metrics and KPIs
   - Powers the dashboard

## Command Line Interface

The main entry point (`main.py`) supports the following arguments:

- `--layer`: Specify which layer to process
  - `bronze`: Process raw data only
  - `silver`: Transform bronze to silver
  - `gold`: Create analytical views
  - `all`: Run complete pipeline (default)

- `--days`: Number of days of data to process (default: 7)

## Development

1. Code Organization:
   - Business logic in `src/etl/`
   - Shared utilities in `src/utils/`
   - Configuration in `src/config/`
   - Dashboard in `src/dashboard/`

2. Data Quality:
   - Validation during extraction
   - Quality checks during transformation
   - Logging of quality metrics

3. Error Handling:
   - Comprehensive error catching
   - Detailed logging
   - Transaction management

## Dashboard

The dashboard provides:
- ETL process monitoring
- Affiliate performance metrics
- Trading volume analytics
- Customer acquisition tracking

Access the dashboard at `http://localhost:8000` after starting the application. 