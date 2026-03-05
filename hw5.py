

    
import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas


# -------------------------------
# Snowflake connection
# -------------------------------
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn


# -------------------------------
# Extract
# -------------------------------
@task
def extract(latitude, longitude):

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"API request failed: {response.status_code}")

    return response.json()


# -------------------------------
# Transform
# -------------------------------

@task
def transform(raw_data, latitude, longitude):

    if "daily" not in raw_data:
        raise ValueError("Missing 'daily' key in API response")

    daily = raw_data["daily"]

    df = pd.DataFrame({
        "LATITUDE": [float(latitude)] * len(daily["time"]),
        "LONGITUDE": [float(longitude)] * len(daily["time"]),
        "DATE": daily["time"],
        "TEMP_MAX": daily["temperature_2m_max"],
        "TEMP_MIN": daily["temperature_2m_min"],
        "PRECIPITATION": daily["precipitation_sum"],
        "WEATHER_CODE": daily["weather_code"]
    })

    # convert DATE safely
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.strftime("%Y-%m-%d")

    print("Data preview:")
    print(df.head())

    return df


# -------------------------------
# Load
# -------------------------------
@task
def load(df):

    conn = return_snowflake_conn()
    cursor = conn.cursor()

    try:

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.WEATHER_HW5 (
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            DATE DATE,
            TEMP_MAX FLOAT,
            TEMP_MIN FLOAT,
            PRECIPITATION FLOAT,
            WEATHER_CODE VARCHAR
        )
        """)

        cursor.execute("DELETE FROM RAW.WEATHER_HW5")

        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name="WEATHER_HW5",
            schema="RAW"
        )

        print(f"Loaded {nrows} rows")

    finally:
        cursor.close()
        conn.close()


# -------------------------------
# DAG
# -------------------------------
with DAG(
    dag_id="HW5",
    start_date=datetime(2026, 2, 23),
    schedule="30 2 * * *",
    catchup=False,
    tags=["ETL"]
) as dag:

    LATITUDE = float(Variable.get("LATITUDE"))
    LONGITUDE = float(Variable.get("LONGITUDE"))

    raw_data = extract(LATITUDE, LONGITUDE)

    df = transform(raw_data, LATITUDE, LONGITUDE)

    load(df)
    
