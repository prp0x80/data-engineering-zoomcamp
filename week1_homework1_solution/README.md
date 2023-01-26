# Week#1 - Homework#1

This homework covers the following topics -

- Practising docker and docker compose basics
- Writing python script to ingest NYC Taxi green trip data into postgres db
- Querying the ingested data using pgAdmin interface

## Usage

```bash
cd week1_homework1_solution/
```

Step#1: Build docker image

```bash
docker build -t taxi_ingest:v1 .
```

Step#2: Create local directory for storing postgres data

```bash
mkdir ny_taxi_postgres_data
```

Step#3: Start postgres and pgAdmin services

```bash
docker compose up -d
```

Step#4: Ingest data

```bash
docker run -it taxi_ingest:v1 \
    --user=root \
    --password=root \
    --host=172.19.0.1 \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL} \
    --file_format=csv
```

> The ip address of the docker host can be found using `ifconfig` in ubuntu

Step#5: Query

Visit `http://localhost:9090` for pgAdmin interface and register your server with same details as in the previous step. Once registered, you can query the data ingested in the tables using the query interface.



