from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import json

# Latitude and longitude for the desired location (e.g., Toronto)
LATITUDE = '43.7'
LONGITUDE = '-79.42'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'weather_api'

default_args ={
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),  # Start from yesterday
}

# Define the DAG
with DAG(dag_id = 'weather_etl_pipeline',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    @task()
    def extract_weather_data():
        """
        Extract weather data from Open-Meteo API using Airflow Connection.
        """

        # Create an HTTP hook to connect to the API
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        # Build the API endpoint URL
        # https://api.open-meteo.com/v1/forecast?latitude=43.7&longitude=-79.42&current_weather=true
        url = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # Make the API request via the HTTP hook
        response = http_hook.run(endpoint=url)
        if response.status_code ==200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")
        
    @task()
    def transform_weather_data(weather_data):
        """
        Transform the extracted weather data to fit the database schema.
        """
        # Extract relevant field from the weather data
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
        } 
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        """
        Load the transformed weather data into a PostgreSQL database.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        #Create table if it does not exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert the transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    # Define the DAG workflow - ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)

    