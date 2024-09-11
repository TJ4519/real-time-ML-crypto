FROM python:3.11.3-slim-buster

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV RUNNING_IN_DOCKER=true  

RUN pip install poetry

WORKDIR /app

COPY . /app

RUN poetry install

CMD ["poetry", "run", "python", "src/main.py"]
