# Crypto Data Platform

A comprehensive platform for processing and analyzing cryptocurrency data, featuring ETL pipelines, data visualization, and API endpoints.

## Features

- Multi-layer ETL pipeline (Bronze, Silver, Gold)
- Bitget exchange integration
- FastAPI dashboard for data visualization
- PostgreSQL database backend
- Docker containerization
- Poetry dependency management

## Project Structure

```
.
├── src/                    # Source code
│   ├── api/               # API endpoints
│   ├── config/            # Configuration files
│   ├── dashboard/         # Dashboard application
│   ├── db/                # Database operations
│   ├── etl/               # ETL pipeline components
│   │   ├── bronze/        # Raw data processing
│   │   ├── silver/        # Data transformation
│   │   └── gold/          # Business views
│   ├── models/            # Data models
│   └── utils/             # Utility functions
├── data/                  # Data storage
├── sql/                   # SQL scripts
├── tests/                 # Test suite
├── logs/                  # Application logs
├── main.py               # Main entry point
├── Dockerfile            # Docker configuration
├── docker-compose.yml    # Docker Compose configuration
├── pyproject.toml        # Poetry dependencies
└── .env                  # Environment variables
```

## Prerequisites

- Python 3.9
- PostgreSQL
- Docker (optional)
- Poetry

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd crypto-data-platform
```

2. Install dependencies using Poetry:
```bash
poetry install
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Usage

### Initialize the DB
```bash
# Setup the database
python src/db/setup.py
```

### Running the ETL Pipeline

The platform supports different ETL layers:

```bash

# Run complete ETL pipeline
python main.py etl --layer all

# Run specific layer
python main.py etl --layer bronze
python main.py etl --layer silver
python main.py etl --layer gold

# Run Bitget ETL
python main.py etl --layer bitget
```

### Starting the Dashboard

```bash
python main.py dashboard --host 0.0.0.0 --port 8000
```

### Docker Deployment

1. Build the Docker image:
```bash
docker build -t chrisgsa/cryptobanter:latest .
```

2. Run using Docker Compose:
```bash
docker-compose up
```

## Configuration

The platform requires the following environment variables:

- `DATABASE_URL`: PostgreSQL connection string
- `BITGET_BASE_URL`: Bitget API base URL
- `BITGET_AFFILIATE_IDS`: Comma-separated list of affiliate IDs
- For each affiliate:
  - `BITGET_AFFILIATE_{ID}_NAME`
  - `BITGET_AFFILIATE_{ID}_API_KEY`
  - `BITGET_AFFILIATE_{ID}_API_SECRET`
  - `BITGET_AFFILIATE_{ID}_API_PASSPHRASE`

## ETL Pipeline

The platform implements a three-layer ETL architecture:

1. **Bronze Layer**: Raw data ingestion and storage
   - Customer data
   - Deposit data
   - Trade data
   - Asset data

2. **Silver Layer**: Data transformation and cleaning
   - Affiliate transformation
   - Customer transformation
   - Deposit transformation
   - Trade transformation

3. **Gold Layer**: Business views and analytics
   - Aggregated views
   - Business metrics
   - Analytics-ready data

## Dashboard

The FastAPI dashboard provides:
- Real-time data visualization
- Affiliate performance metrics
- Trading analytics
- Asset management views

## Testing

Run the test suite:
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

[Add your license information here]
