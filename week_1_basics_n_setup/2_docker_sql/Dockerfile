FROM python:3.9.13

RUN apt-get install wget

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app/pipeline
COPY pipeline .

ENTRYPOINT [ "python", "ingest_data.py" ]