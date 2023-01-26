FROM python:3.9

RUN apt-get install wget
RUN pip install pandas psycopg2-binary pyarrow sqlalchemy

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]