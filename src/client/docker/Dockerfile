# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app/velozefClient

# Install dependencies (system-level)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install pip dependencies (Python libraries)
RUN pip install --upgrade pip

# Install Django
RUN pip install django

# Install cassandra driver for python
RUN pip install cassandra-driver

# Expose the port for Django app (default 8000)
EXPOSE 8000

# Command to keep the container running and launch bash for development
CMD ["python" , "manage.py" , "runserver" , "0.0.0.0:8000"]
