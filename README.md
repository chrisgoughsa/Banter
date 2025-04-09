# Crypto Banter ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for processing data from the Bitget Broker API.

## Project Structure

```
.
├── src/                    # Source code
│   ├── etl/               # ETL pipeline components
│   ├── api/               # API interaction code
│   └── utils/             # Utility functions
├── config/                # Configuration files
├── sql/                   # SQL scripts
│   ├── tables/           # Table creation scripts
│   └── views/            # View creation scripts
├── tests/                 # Test files
│   ├── unit/             # Unit tests
│   └── integration/      # Integration tests
├── docs/                  # Documentation
├── data/                  # Data storage
│   ├── raw/              # Raw data
│   └── processed/        # Processed data
└── logs/                  # Log files
```

## Setup Instructions

1. Install Poetry (if not already installed):
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Clone the repository and install dependencies:
```bash
git clone <repository-url>
cd crypto-banter-etl
poetry install
```

3. Configure your environment:
- Copy `config/config.example.yaml` to `config/config.yaml`
- Update the configuration with your Bitget API credentials

4. Run the ETL pipeline:
```bash
poetry run python src/etl/main.py
```

## Development

### Running Tests
```bash
poetry run pytest
```

### Code Formatting
```bash
# Format code with black
poetry run black .

# Sort imports with isort
poetry run isort .

# Type checking with mypy
poetry run mypy .

# Lint with flake8
poetry run flake8
```

### Adding New Dependencies
```bash
# Add a production dependency
poetry add <package-name>

# Add a development dependency
poetry add --group dev <package-name>
```

## Features

- Extracts data from Bitget Broker API for 10 affiliate accounts
- Transforms raw data into structured format
- Loads data into a database system
- Supports dashboard requirements for KPI tracking

## Database Schema

The database schema is defined in the SQL scripts under `sql/tables/`. The schema is designed to support:
- Affiliate account tracking
- Transaction history
- KPI calculations
- Dashboard requirements 