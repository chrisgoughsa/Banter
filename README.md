# Crypto Data Platform

A comprehensive platform for managing and analyzing cryptocurrency data, with a focus on affiliate tracking and reporting.

## Features

- Multi-layer ETL pipeline (Bronze, Silver, Gold)
- Real-time data extraction from Bitget API
- Automated data transformation and loading
- Interactive dashboard for data visualization
- Affiliate performance tracking
- Customer activity monitoring

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/crypto-data-platform.git
cd crypto-data-platform
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Usage

The platform provides several commands through the main script:

### ETL Pipeline

Run the complete ETL pipeline:
```bash
python main.py etl --layer all --days 7
```

Run specific layers:
```bash
# Extract data from Bitget API
python main.py etl --layer bitget

# Process bronze layer (raw data)
python main.py etl --layer bronze --days 7

# Transform to silver layer
python main.py etl --layer silver

# Create gold layer views
python main.py etl --layer gold
```

### Dashboard

Start the dashboard server:
```bash
# Default host and port
python main.py dashboard

# Custom host and port
python main.py dashboard --host 127.0.0.1 --port 8080
```

### Database Management

Reset the database:
```bash
python main.py reset
```

## Command Reference

| Command | Description | Options |
|---------|-------------|---------|
| `etl` | Run ETL pipeline | `--layer {bronze,silver,gold,all,bitget}`<br>`--days <number>` |
| `dashboard` | Start dashboard server | `--host <hostname>`<br>`--port <port>` |
| `reset` | Reset database | None |

## Project Structure

```
.
├── src/
│   ├── api/              # API clients
│   ├── config/           # Configuration
│   ├── dashboard/        # Dashboard application
│   ├── etl/              # ETL pipeline
│   │   ├── bronze/       # Raw data layer
│   │   ├── silver/       # Transformed data layer
│   │   └── gold/         # Aggregated views
│   ├── models/           # Data models
│   └── utils/            # Utility functions
├── data/                 # Data storage
├── tests/                # Test suite
└── docs/                 # Documentation
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 