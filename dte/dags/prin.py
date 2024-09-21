from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import requests

# API Key de OpenWeather
api_key = "ba7260fc7c657b11720a81e5471e7d14"

# Función para extraer datos de la API pública
def extract_from_api():
    city = "London"  
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url)
    data = response.json()


    weather_data = {
        "city": data["name"],
        "temperature": data["main"]["temp"],
        "weather": data["weather"][0]["description"],
        "timestamp": datetime.utcfromtimestamp(data["dt"]).strftime('%Y-%m-%d %H:%M:%S')
    }

    df_api = pd.DataFrame([weather_data])
    return df_api

# Función para extraer datos de Redshift
def extract_from_db():
    conn = psycopg2.connect(
        dbname='data-engineer-database',
        user='gabrielpin25_coderhouse',
        password='00b99Uzw8s',
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        port='5439'
    )

    query = "SELECT * FROM weather_data LIMIT 100;"
    df_db = pd.read_sql_query(query, conn)
    conn.close()

    return df_db

# Función para combinar los datos y cargarlos en un CSV
def extract_and_load_weather_data():

    df_api = extract_from_api()


    df_db = extract_from_db()


    df_combined = pd.concat([df_api, df_db], ignore_index=True)


    df_combined.to_csv('/usr/local/airflow/dags/combined_weather_data.csv', index=False)

# Función para enviar alertas si la temperatura excede un límite
def send_alert_if_needed():
    df = pd.read_csv('/usr/local/airflow/dags/combined_weather_data.csv')


    temp_limit = 300  
    alert_rows = df[df['temperature'] > temp_limit]

    if not alert_rows.empty:
        
        print(f"Alerta: temperatura en {alert_rows['city'].iloc[0]} ha excedido el límite")

# Definir los parámetros por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'weather_data_dag',
    default_args=default_args,
    description='DAG para extraer y cargar datos de clima diariamente desde API y Redshift',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Definir las tareas del DAG
extract_load_task = PythonOperator(
    task_id='extract_and_load_weather_data',
    python_callable=extract_and_load_weather_data,
    dag=dag,
)

alert_task = PythonOperator(
    task_id='send_alert_if_needed',
    python_callable=send_alert_if_needed,
    dag=dag,
)

# Definir el orden de ejecución de las tareas
extract_load_task >> alert_task
