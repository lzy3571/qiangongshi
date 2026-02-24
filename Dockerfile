FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (e.g. for pandas/numpy)
# RUN apt-get update && apt-get install -y ...

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 5000

# Command to run the application
CMD ["python", "app.py"]
