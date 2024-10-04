FROM python:3.11.3-slim-buster

# Stream output to console
ENV PYTHONUNBUFFERED=1

# Add /app/src to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app/src"

# Install Poetry inside the container
RUN pip install poetry==1.8.3

WORKDIR /app

# Copy pyproject.toml, poetry.lock, and README.md to leverage Docker caching
COPY pyproject.toml poetry.lock README.md /app/

# Install Python dependencies
RUN poetry install

# Copy the entire src directory (to ensure all packages and modules are included)
COPY src /app/src

CMD ["poetry", "run", "python", "src/main.py"]



# Slighly newer version-but still doesn't work namely the ModuleNotFoundError: No module named 'src.kraken_api', make: *** [Makefile:31: run-historical] Error 1
# FROM python:3.11.3-slim-buster

# # stream output to console
# ENV PYTHONUNBUFFERED=1

# # add /app/src to PYTHONPATH

# ENV PYTHONPATH "${PYTHONPATH}:/app/src" 

# #ENV PYTHONPATH=/app

# # install poetry inside the container
# RUN pip install poetry==1.8.3

# WORKDIR /app

# # Copy pyproject.toml, poetry.lock, and README.md to leverage Docker caching
# COPY pyproject.toml poetry.lock README.md /app/

# # install Python dependencies from the pyproject.toml file
# RUN poetry install

# # copy the source code
# COPY src/*.py /app/src/

# CMD ["poetry", "run", "python", "src/main.py"]



# # Old Dockerfile
# FROM python:3.11.3-slim-buster

# #Env variables loading
# ENV PYTHONPATH=/app

# # Stream output to console
# ENV PYTHONUNBUFFERED=1

# # Indicate that the app is running in Docker
# ENV RUNNING_IN_DOCKER=true  

# # Install poetry inside the container
# RUN pip install poetry

# WORKDIR /app

# # Copy all the source code into the container
# COPY . /app

# # Install Python dependencies from the pyproject.toml file
# RUN poetry install

# CMD ["poetry", "run", "python", "src/main.py"]
