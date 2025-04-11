# Use Python 3.9 slim image with explicit platform
FROM --platform=linux/amd64 python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies and PostgreSQL
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    postgresql \
    postgresql-contrib \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Find PostgreSQL version and add to PATH
RUN PG_VERSION=$(ls /usr/lib/postgresql/) && \
    echo "PostgreSQL version: $PG_VERSION" && \
    echo "export PATH=/usr/lib/postgresql/$PG_VERSION/bin:\$PATH" >> ~/.bashrc

# Create necessary directories
RUN mkdir -p /app/data/bronze && \
    mkdir -p /app/data/silver && \
    mkdir -p /app/data/gold && \
    mkdir -p /app/logs && \
    chown -R postgres:postgres /app/data

# Install Poetry using pip
RUN pip install poetry==1.6.1

# Copy poetry files
COPY pyproject.toml poetry.lock* ./

# Configure Poetry
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-interaction --no-ansi --no-root


# Copy the rest of the application
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV RENDER=true

# Set database environment variables
ENV DB_HOST=localhost
ENV DB_PORT=5432
ENV DB_NAME=crypto_data
ENV DB_USER=postgres
ENV DB_PASSWORD=postgres
ENV RENDER=true

# Initialize PostgreSQL
RUN PG_VERSION=$(ls /usr/lib/postgresql/) && \
    mkdir -p /var/lib/postgresql/data && \
    chown -R postgres:postgres /var/lib/postgresql/data && \
    chmod 700 /var/lib/postgresql/data && \
    su postgres -c "/usr/lib/postgresql/$PG_VERSION/bin/initdb -D /var/lib/postgresql/data" && \
    echo "host all all 0.0.0.0/0 trust" >> /var/lib/postgresql/data/pg_hba.conf && \
    echo "listen_addresses = '*'" >> /var/lib/postgresql/data/postgresql.conf

# Create startup script
RUN echo '#!/bin/bash\n\
PG_VERSION=$(ls /usr/lib/postgresql/)\n\
\n\
# Start PostgreSQL\n\
su postgres -c "/usr/lib/postgresql/$PG_VERSION/bin/pg_ctl -D /var/lib/postgresql/data start"\n\
\n\
# Wait for PostgreSQL to start\n\
until su postgres -c "/usr/lib/postgresql/$PG_VERSION/bin/pg_isready"; do\n\
  echo "Waiting for PostgreSQL to start..."\n\
  sleep 1\n\
done\n\
\n\
# Create database\n\
su postgres -c "createdb crypto_data || true"\n\
\n\
# Setup database\n\
echo "Setting up database..."\n\
poetry run python -c "from src.db.setup import setup_database; from src.db.connection import create_db_connection; setup_database(create_db_connection())"\n\
\n\
# Run regular ETL process\n\
echo "Running regular ETL process..."\n\
poetry run python main.py etl --layer all\n\
\n\
# Start dashboard\n\
echo "Starting dashboard..."\n\
poetry run python main.py dashboard --host 0.0.0.0 --port ${PORT:-8000}' > /app/start.sh && \
    chmod +x /app/start.sh

# Expose the port the app runs on
EXPOSE 8000

# Command to run the application
CMD ["/app/start.sh"]
