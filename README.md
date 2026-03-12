# Open_Meteo_API

This project implements a Dockerized Apache Airflow ETL pipeline that ingests weather data for San Jose from the Open-Meteo API, performs Pandas-based transformations, and loads the results into Snowflake using the SnowflakeHook. The repository contains two versions of the pipeline developed across HW5 and HW6. In HW5, the pipeline performs a full refresh load, while in HW6 the pipeline was extended to support incremental updates using SQL transactions.

The pipeline runs inside Docker using docker-compose, and the Airflow UI can be accessed at [http://localhost:8081](http://localhost:8081) after starting the services with `docker compose up -d`. The Airflow environment requires a Snowflake connection named `open_meteo_api`, as well as Airflow Variables named `LATITUDE` and `LONGITUDE` for the location coordinates.

In HW5, the DAG performs a full ETL workflow where weather data is extracted from the Open-Meteo API, saved to a CSV file, uploaded to a Snowflake stage, and then loaded into a Snowflake table using COPY INTO. This version always refreshes the entire table and stores the results in the table RAW.WEATHER_DATA_HW5.

In HW6, the DAG was modified to support incremental loading. Instead of reloading the entire table, the pipeline uses Airflow’s logical_date to fetch data for one day at a time. Before loading new data, existing records for that date are deleted to avoid duplicates. The load step is wrapped in a SQL transaction using BEGIN, COMMIT, and ROLLBACK to ensure data consistency. A temporary Snowflake stage is used to upload the CSV file, and the data is copied into the target table RAW.WEATHER_DATA_HW6. This DAG runs on a daily schedule and ensures that only new data is added while keeping previous records intact.

The project demonstrates the use of Apache Airflow, Docker Compose, Snowflake, Pandas, Requests, the Open-Meteo API, and SnowflakeHook to build both full-refresh and incremental ETL pipelines.
