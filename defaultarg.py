from airflow import DAG
from airflow.operators.bash import BashOperator  #Import correto no Airflow 2.x
from datetime import datetime, timedelta

#Definição da lista
default_args = {
    'depends_on_past' : False,
    'start_date' : datetime(2024,9,30),
    'email' : ['test@test.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(seconds=10)
}

# Definição da DAG
dag = DAG('defaultargs', description='dag de exemplo',
        default_args = default_args,
        schedule_interval='@hourly', start_date=datetime(2024,9,28),
        catchup=False, default_view='graph', tags=['processo', 'tag', 'pipeline'])

# Definição das Task1
task1 = BashOperator(task_id='task1', bash_command='sleep 5', dag=dag, retries=3 ) 
task2 = BashOperator(task_id='task2', bash_command='sleep 5', dag=dag) 
task3 = BashOperator(task_id='task3', bash_command='sleep 5', dag=dag) 

task1 >> task2 >> task3
