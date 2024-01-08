from datetime import datetime
from datetime import timezone
import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator



cities_coordinates = {
        "Lviv": (49.839684, 24.029716),
        "Kyiv": (50.447731, 30.542721),
        "Odesa": (46.476608, 30.707310),
        "Kharkiv": (49.993500, 36.230385),
        "Zhmerynka": (49.037560, 28.108900)
}

def _get_weather_data(ti, city):
    task_id = f"extract_api_data_{city}"
    data = ti.xcom_pull(task_id)

    timestamp = data["data"][0]["dt"]
    temperature = data["data"][0]["temp"]
    humidity = data["data"][0]["humidity"]
    clouds = data["data"][0]["clouds"]
    wind_speed = data["data"][0]["wind_speed"]

    return city, timestamp, temperature, humidity, clouds, wind_speed

def _get_data_for_query(ti, ds):
    timestamp = int(
        datetime.strptime(ds, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    return str(timestamp)

with DAG(dag_id="hw1_oleh_palka", schedule_interval="@daily", start_date=datetime(2023, 11, 18), catchup=True) as dag:
    dt_create = SqliteOperator(
        task_id="create_sqlite_dt",
        sqlite_conn_id="airflow_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS weather_measures
        (
        City TEXT,
        Timestamp TIMESTAMP,
        Temperature FLOAT,
        Humidity INT,
        Clouds TEXT,
        Wind_speed FLOAT
        );"""
    )

    for city in cities_coordinates:

        ver_api = HttpSensor(
            task_id=f"ver_api_{city}",
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall",
            request_params={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": str(cities_coordinates[city][0]),
                "lon": str(cities_coordinates[city][1])
            }
        )

        get_data_for_query = PythonOperator(
            task_id=f"get_data_for_query_{city}", python_callable=_get_data_for_query
        )

        get_data = SimpleHttpOperator(
            task_id=f"get_data_{city}",
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": str(cities_coordinates[city][0]),
                "lon": str(cities_coordinates[city][1]),
                "dt": f"""{{{{ti.xcom_pull(task_ids='get_data_{city}')}}}}"""
            },
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True
        )

        
        process_data = PythonOperator(
            task_id=f"process_data_{city}",
            python_callable=_get_weather_data,
            op_kwargs={'city': city},
        )

        insert_data_to_db = SqliteOperator(
            task_id=f"insert_data_to_db{city}",
            sqlite_conn_id="airflow_conn",
            sql="""
                INSERT INTO weather_measures (City, Еimestamp, Temperature, Рumidity, Clouds, Wind_speed) 
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
            parameters=[
                "{{ti.xcom_pull(task_ids='process_data_" + city + "')[0]}}",
                "{{ti.xcom_pull(task_ids='process_data_" + city + "')[1]}}",
                "{{ti.xcom_pull(task_ids='process_data_" + city + "')[2]}}",
                "{{ti.xcom_pull(task_ids='process_data_" + city + "')[3]}}",
                "{{ti.xcom_pull(task_ids='process_data_" + city + "')[4]}}",
                "{{ti.xcom_pull(task_ids='process_data_" + city + "')[5]}}",
            ],
        )

        dt_create >> ver_api >> get_data_for_query >> get_data >> process_data >> insert_data_to_db