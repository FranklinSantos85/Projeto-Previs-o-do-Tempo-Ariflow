from airflow import DAG
from airflow.operators.bash import BashOperator  # Import correto no Airflow 2.x
from datetime import datetime

# Definição da DAG
dag = DAG('trigger_dag', description='Trigger',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)

# Definição das Tasks
task1 = BashOperator(task_id='task1', bash_command='sleep 5', dag=dag)
task2 = BashOperator(task_id='task2', bash_command='sleep 5', dag=dag)
task3 = BashOperator(task_id='task3', bash_command='sleep 5', dag=dag,
                         trigger_rule='one_failed' ) # one_failed, espera que alguma das tasks falhe para ser executada a tastk 3

[task1,task2] >> task3
