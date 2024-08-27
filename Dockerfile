FROM python:3.10.12-slim

# Install necessary packages
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql \
    postgresql-client \
    libpq-dev

# Create application directory
RUN mkdir /app
WORKDIR /app

# Copy and install Python dependencies
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV PYTHONUNBUFFERED 1

# Copy application code
COPY . .
