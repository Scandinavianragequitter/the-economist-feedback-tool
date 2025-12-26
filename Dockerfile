# Use the official Python image as a base
FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=api_proxy.py

# Create a directory for the app code
WORKDIR /usr/src/app

# Copy dependency files and install Python dependencies
# Using a virtual environment to manage dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the configuration files (assuming config is a folder)
# IMPORTANT: You must ensure your API keys are managed safely (e.g., using environment variables in production)
COPY config/ /usr/src/app/config/

# Copy all application code
COPY . .

# Expose the port the app runs on. 
# Changed from 5000 to 10000 (Render's internal port) for documentation.
EXPOSE 10000

# Command to run the application
# **CRITICAL FIX**: Changed port from 5000 to use the environment variable $PORT
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "api_proxy:app"]