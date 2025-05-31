# Weather ETL Pipeline using Apache Airflow

## Project Overview

This project implements an **ETL (Extract, Transform, Load) pipeline** using **Apache Airflow**. The DAG fetches **current weather data** from the [Open-Meteo API](https://open-meteo.com/), transforms it, and loads it into a **PostgreSQL database**.

The pipeline is scheduled to run **daily**, automatically performing:
- Data extraction from an external API
- Transformation into a structured format
- Loading into a database table

---

## Technologies & Tools

- **Apache Airflow**
- **Python 3.10+**
- **PostgreSQL**
- **Docker + Astro CLI**
- **Open-Meteo API**
- **Airflow Providers:**
  - `apache-airflow-providers-http`
  - `apache-airflow-providers-postgres`

---

## DAG Structure

```plaintext
weather_etl_pipeline
│
├── extract_weather_data()    # Fetches weather data via HTTP
├── transform_weather_data()  # Parses and formats the data
└── load_weather_data()       # Writes data to PostgreSQL
