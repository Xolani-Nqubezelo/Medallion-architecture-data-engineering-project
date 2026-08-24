FROM python:3.11-slim

LABEL maintainer="medallion-architecture-project"
LABEL description="dbt Databricks environment for Medallion Architecture project"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /project

# Copy dependency manifest first for layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create dbt profiles directory (mount your actual profiles.yml at runtime)
RUN mkdir -p /root/.dbt

# Default command: show dbt version and project info
CMD ["dbt", "--version"]
