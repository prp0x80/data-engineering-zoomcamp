1. What is the count for fhv vehicle records for year 2019?

```sql
SELECT COUNT(*) FROM `fhv_2019.fhv_data`
```

> 43,244,696


2. Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql
-- create an external table
CREATE OR REPLACE EXTERNAL TABLE `fhv_2019.fhv_external`
OPTIONS (
    format = 'CSV',
    uris = ['gs://dtc_data_lake_agile-extension-375516/data/fhv/*.csv.gz']
);

-- count using normal table
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `fhv_2019.fhv_data`

-- count using external table
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `fhv_2019.fhv_external` 
```

> 0 MB for the External Table and 317.94MB for the BQ Table

3. How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

```sql
SELECT COUNT(*) FROM `fhv_2019.fhv_data` WHERE PUlocationID IS NULL AND DOlocationID IS NULL
```
> 717,748

4. What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

The best strategy would be to `partition by pickup_datetime and cluster on affiliated_base_number`. This is because the queries always filter by pickup_datetime, so partitioning on this column will ensure that only the relevant partitions are scanned during query execution. Additionally, clustering on affiliated_base_number will group together similar values, making it easier and faster for the query engine to process the data.

> Partition by pickup_datetime Cluster on affiliated_base_number

5. Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).

```sql

-- create a partitioned table
CREATE OR REPLACE TABLE fhv_2019.fhv_optimised
PARTITION BY DATE(pickup_datetime) 
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `fhv_2019.fhv_data`;

-- query on partitioned table
SELECT DISTINCT Affiliated_base_number
FROM `fhv_2019.fhv_optimised`
WHERE pickup_datetime >= '2019-03-01' AND pickup_datetime <= '2019-04-01'

-- query on non-partitioned table
SELECT DISTINCT Affiliated_base_number
FROM `fhv_2019.fhv_data`
WHERE pickup_datetime >= '2019-03-01' AND pickup_datetime <= '2019-04-01'
```

> 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

6. Where is the data stored in the External Table you created?

For external table, the data is stored in `GCP Bucket`

> GCP Bucket

7. It is best practice in Big Query to always cluster your data - True or False?

It is **NOT** a best practice in Big Query to always cluster your data. Specifically, if the data is less than 1 GB, clustering can be an overhead.

> False



