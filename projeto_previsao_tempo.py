from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
import requests
import json
import psycopg2

# Definir a DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
    'retries': 1
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Pipeline de coleta e processamento de dados do clima',
    schedule_interval='@daily',
    catchup=False
)

# Função para extrair dados da API
def extract_weather_data():
    api_key = 'add_seu_key'
    city = 'Guarulhos'
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    response = requests.get(url)
    data = response.json()

    with open('/tmp/weather_data.json', 'w') as outfile:
        json.dump(data, outfile)

# Função para transformar os dados
def transform_weather_data():
    with open('/tmp/weather_data.json') as json_file:
        data = json.load(json_file)

    processed_data = {
        'city': data['name'],
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'weather': data['weather'][0]['description']
    }

    with open('/tmp/processed_weather_data.json', 'w') as outfile:
        json.dump(processed_data, outfile)

# Função para carregar os dados no PostgreSQL
def load_data_to_postgres():
    conn = psycopg2.connect(
        host="add_seu_host",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    with open('/tmp/processed_weather_data.json') as json_file:
        data = json.load(json_file)

    cursor.execute(
        "INSERT INTO weather (city, temperature, humidity, weather) VALUES (%s, %s, %s, %s)",
        (data['city'], data['temperature'], data['humidity'], data['weather'])
    )
    
    conn.commit()
    cursor.close()
    conn.close()

# Definir as tasks
t1 = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag
)

t2 = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_weather_data,
    dag=dag
)

t3 = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag
)

# Definir ordem das tasks
t1 >> t2 >> t3
