FROM python:3.11.3-slim-buster


ENV PYTHONPATH=/app

# Stream output to console
ENV PYTHONUNBUFFERED=1

# Indicate that the app is running in Docker
ENV RUNNING_IN_DOCKER=true  

# Install poetry inside the container
RUN pip install poetry

WORKDIR /app

# Copy all the source code into the container
COPY . /app

# Install Python dependencies from the pyproject.toml file
RUN poetry install

CMD ["poetry", "run", "python", "src/main.py"]
