## Formula1 Racing Project

This folder contains various notebooks that have been used to perform data ingestion and various data transformations using real world formula1 data.

#### Data Ingestion Requirements
1. Ingest all the files from the data lake.
2. Ingested data must have the schema applied.
3. Ingested data must have audit columns.
4. Ingested data must be stored in columnar format (i.e Parquet).
5. Must be able to analyze the ingested data via SQL.
6. Ingestion logic must be able to handle incremental load.

#### Data Transformation Requirements
1. Join the key information required for reporting to create a new table.
2. Join the key information required for analysis to create a new table.
3. Transformed tables must have audit columns.
4. Must be able to analyze the transformed data via SQL.
5. Transformed data must be stored in columnar format (i.e Parquet.)
6. Tranformation logic must be able to handle incremental load.

#### Reporting Requirements
1. Driver standings.
2. Constructor standings.

#### Analysis Requirements
1. Dominant drivers.
2. Dominant teams.
3. Visualize the outputs.
4. Create Databricks Dashboards.

### Solution Architecture

![Solution Architecture](formula1_project/Solution_Architecture.png)

###### Credits
This project has been inspired by Ramesh Retnasamy Udemy course on Databricks.


