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

2. Install Poetry if you haven't already:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

3. Install dependencies:
```bash
poetry install
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Local Development with Docker

1. Build and start the containers:
```bash
docker-compose up --build
```

2. Access the application:
- Web app: http://localhost:8000
- Database: localhost:5432

3. Run ETL processes:
```bash
docker-compose exec web poetry run python main.py etl --layer all
```

## Deployment to Heroku

1. Install the Heroku CLI:
```bash
# macOS
brew tap heroku/brew && brew install heroku

# Windows
choco install heroku-cli
```

2. Login to Heroku:
```bash
heroku login
```

3. Create a new Heroku app:
```bash
heroku create your-app-name
```

4. Add PostgreSQL addon:
```bash
heroku addons:create heroku-postgresql:hobby-dev
```

5. Set environment variables:
```bash
heroku config:set \
  DB_HOST=$(heroku config:get DATABASE_URL | cut -d@ -f2 | cut -d/ -f1) \
  DB_NAME=$(heroku config:get DATABASE_URL | cut -d/ -f4) \
  DB_USER=$(heroku config:get DATABASE_URL | cut -d/ -f3 | cut -d: -f1) \
  DB_PASSWORD=$(heroku config:get DATABASE_URL | cut -d/ -f3 | cut -d: -f2 | cut -d@ -f1)
```

6. Deploy to Heroku:
```bash
git push heroku main
```

7. Run database migrations:
```bash
heroku run poetry run python main.py reset
```

8. Run ETL process:
```bash
heroku run poetry run python main.py etl --layer all
```

## Usage

The platform provides several commands through the main script:

### ETL Pipeline

Run the complete ETL pipeline:
```bash
poetry run python main.py etl --layer all --days 7
```

Run specific layers:
```bash
# Extract data from Bitget API
poetry run python main.py etl --layer bitget

# Process bronze layer (raw data)
poetry run python main.py etl --layer bronze --days 7

# Transform to silver layer
poetry run python main.py etl --layer silver

# Create gold layer views
poetry run python main.py etl --layer gold
```

### Dashboard

Start the dashboard server:
```bash
# Default host and port
poetry run python main.py dashboard

# Custom host and port
poetry run python main.py dashboard --host 127.0.0.1 --port 8080
```

### Database Management

Reset the database:
```bash
poetry run python main.py reset
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