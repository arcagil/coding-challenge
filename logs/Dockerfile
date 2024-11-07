FROM python:3.11-slim

WORKDIR /app

# Install poetry
RUN pip install poetry

# Copy poetry files
COPY pyproject.toml poetry.lock ./

# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Copy application code
COPY app/ ./app/
COPY alembic.ini ./
COPY alembic/ ./alembic/
COPY generator.py ./

# Copy entrypoint script
COPY entrypoint.sh ./

# Make script executable and ensure proper line endings
RUN chmod +x entrypoint.sh && \
    sed -i 's/\r$//' entrypoint.sh && \
    chmod 755 entrypoint.sh

ENTRYPOINT ["sh", "/app/entrypoint.sh"]