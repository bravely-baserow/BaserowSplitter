# 1. Use an official Python runtime as a parent image
FROM python:3.11-slim

# 2. Set the working directory in the container
WORKDIR /app

# 3. Copy your requirements file first (for better caching)
COPY requirements.txt .

# 4. Install any needed packages
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the rest of your code
COPY . .

# 6. Run your script when the container starts
CMD ["python", "baserow-splitter.py"]
