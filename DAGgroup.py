from airflow import DAG
from airflow.operators.bash import BashOperator  # Import correto no Airflow 2.x
from datetime import datetime
from airflow.utils.task_group import TaskGroup # Import do modulo TaskGroup para simplificar o código

# Definição da DAG
dag = DAG('DAGgroup', description='group',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)

# Definição das Task1
task1 = BashOperator(task_id='task1', bash_command='sleep 5', dag=dag) 
task2 = BashOperator(task_id='task2', bash_command='sleep 5', dag=dag) 
task3 = BashOperator(task_id='task3', bash_command='sleep 5', dag=dag)
task4 = BashOperator(task_id='task4', bash_command='sleep 5', dag=dag)
                
task5 = BashOperator(task_id='task5', bash_command='sleep 5', dag=dag)
task6 = BashOperator(task_id='task6', bash_command='sleep 5', dag=dag)

tsk_group = TaskGroup("tsk_group", dag=dag) 

task7 = BashOperator(task_id='task7', bash_command='sleep 5', dag=dag, task_group=tsk_group )
task8 = BashOperator(task_id='task8', bash_command='sleep 5', dag=dag, task_group=tsk_group )
task9 = BashOperator(task_id='task9', bash_command='sleep 5', dag=dag,
                     trigger_rule='one_failed', task_group=tsk_group  )

task1 >> task2
task3 >> task4
[task2, task4] >> task5 >> task6
task6 >> tsk_group