# Use an official Python runtime as the base image
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any dependencies
RUN pip install poetry
RUN poetry install

# Define the command to run when the container starts
CMD ["python", "app.py"]