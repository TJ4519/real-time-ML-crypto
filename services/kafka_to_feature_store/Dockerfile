FROM python:3.11.3-slim-buster

# Install necessary build tools and libraries
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    build-essential \
    libssl-dev
# Stream output to console
ENV PYTHONUNBUFFERED=1

# Add /app/src to PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/app/src"

# Install Poetry inside the container
RUN pip install poetry==1.8.3

WORKDIR /app

# Copy pyproject.toml, poetry.lock, and README.md to leverage Docker caching
COPY pyproject.toml poetry.lock README.md /app/

# # Install 'twofish' independently due to PEP 517 issues
# RUN pip install --no-cache-dir twofish==0.3.0

# Install Python dependencies from the pyproject.toml file
RUN poetry install

# Copy the entire src directory (to ensure all packages and modules are included)
COPY src /app/src

CMD ["poetry", "run", "python", "src/main.py"]

#Slightly newer but doesn't work (Error1)
# FROM python:3.11.3-slim-buster

# # install gcc and python3-dev to compile Python packages
# RUN apt-get update && apt-get install -y \
#     gcc \
#     python3-dev

# # stream output to console
# ENV PYTHONUNBUFFERED=1

# # add /app/src to PYTHONPATH
# ENV PYTHONPATH "${PYTHONPATH}:/app/src"

# # install poetry inside the container
# RUN pip install poetry==1.8.2

# WORKDIR /app

# # copy only dependency files first to leverage Docker caching
# COPY pyproject.toml poetry.lock /app/

# # install Python dependencies from the pyproject.toml file
# RUN poetry install

# # copy the source code
# COPY src/*.py /app/src/

# CMD ["poetry", "run", "python", "src/main.py"]

















# #Old
# FROM python:3.11.3-slim-buster

# ENV PYTHONPATH=/app
# ENV PYTHONUNBUFFERED=1
# ENV RUNNING_IN_DOCKER=true  

# RUN pip install poetry

# WORKDIR /app

# COPY . /app

# RUN poetry install

# CMD ["poetry", "run", "python", "src/main.py"]
