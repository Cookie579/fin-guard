# Use a lightweight Python image
FROM python:3.12-slim

# Set work directory inside container
WORKDIR /app

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code into image
COPY python/ ./python/

# Change to app folder if needed
WORKDIR /app/python

# Run your app — update this line based on your framework
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
