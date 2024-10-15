from airflow import DAG
from airflow.operators.bash import BashOperator  # Import correto no Airflow 2.x
from datetime import datetime

# Definição da DAG
dag = DAG('DAGcomplexa', description='complexa',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)

# Definição das Task1
task1 = BashOperator(task_id='task1', bash_command='sleep 5', dag=dag) 
task2 = BashOperator(task_id='task2', bash_command='sleep 5', dag=dag) 
task3 = BashOperator(task_id='task3', bash_command='sleep 5', dag=dag)
task4 = BashOperator(task_id='task4', bash_command='sleep 5', dag=dag)
                
task5 = BashOperator(task_id='task5', bash_command='sleep 5', dag=dag)
task6 = BashOperator(task_id='task6', bash_command='sleep 5', dag=dag)

task7 = BashOperator(task_id='task7', bash_command='sleep 5', dag=dag)
task8 = BashOperator(task_id='task8', bash_command='sleep 5', dag=dag)
task9 = BashOperator(task_id='task9', bash_command='sleep 5', dag=dag,
                     trigger_rule='one_failed' )

task1 >> task2
task3 >> task4
[task2, task4] >> task5 >> task6
task6 >> [task7, task8, task9]