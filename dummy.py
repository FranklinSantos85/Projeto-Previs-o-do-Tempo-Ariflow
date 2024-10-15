from airflow import DAG
from airflow.operators.bash import BashOperator  #Import correto no Airflow 2.x
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime


# Definição da DAG
dag = DAG('dummy', description='dag dummy',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)

# Definição das Task1
task1 = BashOperator(task_id='task1', bash_command='sleep 1', dag=dag) 
task2 = BashOperator(task_id='task2', bash_command='sleep 1', dag=dag) 
task3 = BashOperator(task_id='task3', bash_command='sleep 1', dag=dag)
task4 = BashOperator(task_id='task4', bash_command='sleep 1', dag=dag)               
task5 = BashOperator(task_id='task5', bash_command='sleep 1', dag=dag)
taskdummy = DummyOperator(task_id='taskdummy', dag=dag)

[task1, task2, task3] >> taskdummy >> [task4, task5]