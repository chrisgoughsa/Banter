# Use Python 3.9 slim image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
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

# Expose the port the app runs on
EXPOSE 8000

# Create startup script
RUN echo '#!/bin/bash\n\
# Wait for database to be ready\n\
echo "Waiting for database to be ready..."\n\
sleep 5\n\
\n\
# Setup database\n\
echo "Setting up database..."\n\
poetry run python -c "from src.utils.db_setup import create_all_tables; from src.db.connection import create_db_connection; create_all_tables(create_db_connection())"\n\
\n\
# Run regular ETL process\n\
echo "Running regular ETL process..."\n\
poetry run python main.py etl --layer all\n\
\n\
# Start dashboard\n\
echo "Starting dashboard..."\n\
poetry run python main.py dashboard --host 0.0.0.0 --port 8000' > /app/start.sh && \
chmod +x /app/start.sh

# Command to run the application
CMD ["/app/start.sh"] 