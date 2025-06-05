# Use a lightweight Python image 3

FROM python:3.12-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY python/ ./python/

# Set working directory to the app folder
WORKDIR /app/python

# Run your FastAPI app from get_apple_stock.py
CMD ["uvicorn", "get_apple_stock:app", "--host", "0.0.0.0", "--port", "8080"]
