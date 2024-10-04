FROM python:3.11.3-slim-buster

# stream output to console
ENV PYTHONUNBUFFERED=1

# add /app/src to PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/app/src"

# install poetry inside the container
RUN pip install poetry==1.8.3

WORKDIR /app

# Copy pyproject.toml, poetry.lock, and README.md to leverage Docker caching
COPY pyproject.toml poetry.lock README.md /app/

# install Python dependencies from the pyproject.toml file
RUN poetry install

# Copy the entire src directory (to ensure all packages and modules are included)
COPY src /app/src

CMD ["poetry", "run", "python", "src/main.py"]




# Old

# FROM python:3.11.3-slim-buster

# ENV PYTHONPATH=/app
# ENV PYTHONUNBUFFERED=1
# ENV RUNNING_IN_DOCKER=true  

# RUN pip install poetry

# WORKDIR /app

# COPY . /app

# RUN poetry install

# CMD ["poetry", "run", "python", "src/main.py"]
