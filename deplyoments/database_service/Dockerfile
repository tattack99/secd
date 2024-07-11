# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . .

# Make sure the __init__.py file is present
RUN touch /app/__init__.py

# Run gunicorn server, binding to 0.0.0.0:8000
CMD ["gunicorn", "app:api", "-b", "0.0.0.0:8000"]
