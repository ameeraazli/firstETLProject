# Use an official Python runtime as the base image
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

COPY pyproject.toml /app
COPY poetry.lock /app
# Install any dependencies
RUN pip3 install poetry
RUN poetry install