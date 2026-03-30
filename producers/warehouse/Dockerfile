FROM python:3.11-slim

WORKDIR /app

# Copy dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer script
COPY main.py .

# Run the producer
CMD ["python", "main.py"]