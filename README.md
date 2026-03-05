# Open_Meteo_API

Built a Dockerized Apache Airflow ETL pipeline that ingests weather data for San Jose from the Open-Meteo API, performs Pandas transformations, and loads results into Snowflake using SnowflakeHook.

How to start: docker compose up -d

Airflow UI: http://localhost:8081 (login airflow/airflow if using default)

Connection setup: open_meteo_api (Snowflake)

Target table: RAW.WEATHER_DATA_HW5
