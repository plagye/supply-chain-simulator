# Use a lightweight Python base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements first to leverage Docker cache
# (Ensure requirements.txt exists in your repo)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port the simulator API runs on (assuming 8000)
EXPOSE 8010

# Command to run the simulator API
# Replace 'main:app' with your actual entry point (e.g., 'src.api:app')
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8010"]
