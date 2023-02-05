FROM python:3.9.13-slim-buster

COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flow /opt/prefect/flow