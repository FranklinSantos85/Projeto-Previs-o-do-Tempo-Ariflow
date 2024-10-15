from airflow import DAG
from airflow.operators.bash import BashOperator  # Import correto no Airflow 2.x
from datetime import datetime

# Definição da DAG
dag = DAG('dagrunDAG2', description='dag run dag',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)

# Definição das Task1
task1 = BashOperator(task_id='task1', bash_command='sleep 5', dag=dag) 
task2 = BashOperator(task_id='task2', bash_command='sleep 5', dag=dag) 


task1 >> task2
