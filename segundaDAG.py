from airflow import DAG
from airflow.operators.bash import BashOperator  # Import correto no Airflow 2.x
from datetime import datetime

# Definição da DAG
dag = DAG(
    'segunda_dag', 
    description='segunda_dag',
    schedule_interval=None, 
    start_date=datetime(2024, 9, 28),
    catchup=False
)

# Definição das Tasks
task1 = BashOperator(
    task_id='task1', 
    bash_command='sleep 5', 
    dag=dag
)

task2 = BashOperator(
    task_id='task2', 
    bash_command='sleep 5', 
    dag=dag
)

task3 = BashOperator(
    task_id='task3', 
    bash_command='sleep 5', 
    dag=dag
)

# Dependência das Tasks 2 e 3 em paralelo
task1 >> [task2, task3]
