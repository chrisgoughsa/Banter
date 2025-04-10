# Crypto Data Platform

A comprehensive data platform for cryptocurrency trading analytics, built with Python and PostgreSQL.

## Features

- Multi-layer ETL pipeline (Bronze → Silver → Gold)
- Bitget Broker API integration
- Real-time dashboard
- Comprehensive analytics
- Data quality monitoring

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Poetry (Python package manager)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/crypto-data-platform.git
cd crypto-data-platform
```

2. Install dependencies:
```bash
poetry install
```

3. Set up environment variables:
```bash
cp .env.example .env
```
Edit `.env` with your configuration:
```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crypto_data
DB_USER=your_user
DB_PASSWORD=your_password

# Bitget API Configuration
BITGET_BASE_URL=https://api.bitget.com
BITGET_AFFILIATE_IDS=affiliate1,affiliate2

# For each affiliate, add their credentials:
BITGET_AFFILIATE_affiliate1_NAME=Affiliate One
BITGET_AFFILIATE_affiliate1_API_KEY=your_api_key
BITGET_AFFILIATE_affiliate1_API_SECRET=your_api_secret
BITGET_AFFILIATE_affiliate1_API_PASSPHRASE=your_passphrase

# Optional: Specific client IDs to process
# BITGET_AFFILIATE_affiliate1_CLIENT_IDS=client1,client2
```

## Usage

### Running the ETL Pipeline

The platform provides a command-line interface for running different parts of the ETL pipeline:

```bash
# Run the complete ETL pipeline (all layers)
python main.py etl --layer all

# Run specific layers
python main.py etl --layer bronze  # Raw data extraction
python main.py etl --layer silver  # Data transformation
python main.py etl --layer gold    # Analytics views
python main.py etl --layer bitget  # Bitget API data extraction

# Process specific time range
python main.py etl --layer all --days 30  # Process last 30 days
```

### Starting the Dashboard

```bash
# Start the dashboard server
python main.py dashboard

# Customize host and port
python main.py dashboard --host 127.0.0.1 --port 8080
```

### ETL Pipeline Details

1. **Bronze Layer**
   - Extracts raw data from Bitget API
   - Rate limited to 10 requests/second
   - Stores data in JSON format
   - Includes metadata and timestamps

2. **Silver Layer**
   - Transforms raw data into structured format
   - Validates and cleans data
   - Creates normalized tables
   - Tracks data quality metrics

3. **Gold Layer**
   - Creates materialized views for analytics
   - Provides aggregated metrics
   - Optimizes query performance
   - Supports concurrent refreshes

### Data Types Processed

- **Customer/Affiliate Data**
  - Registration information
  - Account status
  - Country data

- **Trade Activities**
  - Trade volumes
  - Symbol information
  - Trade types (buy/sell)

- **Deposits**
  - Deposit amounts
  - Coin types
  - Transaction status

- **Assets**
  - Account balances
  - Asset types
  - Update timestamps

## Development

### Project Structure

```
.
├── src/
│   ├── api/           # API clients
│   ├── config/        # Configuration
│   ├── dashboard/     # Web dashboard
│   ├── db/           # Database utilities
│   ├── etl/          # ETL pipeline
│   ├── models/       # Data models
│   └── utils/        # Utility functions
├── sql/              # SQL scripts
├── tests/            # Test suite
└── docs/             # Documentation
```

### Running Tests

```bash
poetry run pytest
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 