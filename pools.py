from airflow import DAG
from airflow.operators.bash_operator import BashOperator  #Import correto no Airflow 2.x
from datetime import datetime

# Definição da DAG
dag = DAG('pool', description='dag pool',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)


# Definição das Task1
task1 = BashOperator(task_id='task1', bash_command='sleep 5', dag=dag, pool='meupool') 
task2 = BashOperator(task_id='task2', bash_command='sleep 5', dag=dag, pool='meupool', priority_weight=5) 
task3 = BashOperator(task_id='task3', bash_command='sleep 5', dag=dag, pool='meupool')
task4 = BashOperator(task_id='task4', bash_command='sleep 5', dag=dag, pool='meupool', priority_weight=10)

