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
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

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

# Create PostgreSQL startup script
RUN echo '#!/bin/bash\n\
# Initialize PostgreSQL data directory\n\
mkdir -p /var/run/postgresql\n\
chown postgres:postgres /var/run/postgresql\n\
mkdir -p /var/lib/postgresql/data\n\
chown postgres:postgres /var/lib/postgresql/data\n\
\n\
# Initialize database if it doesnt exist\n\
if [ ! -f /var/lib/postgresql/data/PG_VERSION ]; then\n\
    su - postgres -c "initdb -D /var/lib/postgresql/data"\n\
    \n\
    # Modify postgresql.conf to listen on all addresses\n\
    su - postgres -c "echo \"listen_addresses = '\\'*\\'\"\" >> /var/lib/postgresql/data/postgresql.conf"\n\
    \n\
    # Add host authentication\n\
    su - postgres -c "echo \"host all all all trust\" >> /var/lib/postgresql/data/pg_hba.conf"\n\
fi\n\
\n\
# Start PostgreSQL\n\
su - postgres -c "pg_ctl -D /var/lib/postgresql/data start"\n\
\n\
# Create database if it doesnt exist\n\
su - postgres -c "createdb -U postgres crypto_data || true"\n\
\n\
# Wait for PostgreSQL to be ready\n\
until su - postgres -c "pg_isready"; do\n\
    echo "Waiting for PostgreSQL to be ready..."\n\
    sleep 1\n\
done\n\
\n\
# Setup database schema\n\
echo "Setting up database..."\n\
poetry run python -c "from src.utils.db_setup import create_all_tables; from src.db.connection import create_db_connection; create_all_tables(create_db_connection())"\n\
\n\
# Run regular ETL process\n\
echo "Running regular ETL process..."\n\
poetry run python main.py etl --layer all\n\
\n\
# Start dashboard\n\
echo "Starting dashboard..."\n\
poetry run python main.py dashboard --host 0.0.0.0 --port $PORT' > /app/start.sh && \
chmod +x /app/start.sh

# Expose the port the app runs on
EXPOSE 8000

# Command to run the application
CMD ["/app/start.sh"]