from airflow import DAG
from airflow.operators.bash import BashOperator  # Import correto no Airflow 2.x
from datetime import datetime

# Definição da DAG
dag = DAG(
    'terceira_dag', 
    description='terceira_dag',
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

# Dependência das Tasks 1 e 2 em paralelo e Tasck 3 em Precedência
[task1, task2] >> task3
