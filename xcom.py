from airflow import DAG
from airflow.operators.bash import BashOperator  #Import correto no Airflow 2.x
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


# Definição da DAG
dag = DAG('dagxcomDAG', description='dag xcom',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)

def task_write(**kwarg):
    kwarg['ti'].xcom_push(key='valorxcom1',value=10200)

# Definição das Task1
task1 = PythonOperator(task_id='tsk1', python_callable=task_write, dag=dag) 

def task_read(**kwarg):
    valor = kwarg['ti'].xcom_pull(key='valorxcom1')
    print(f"valor recuperado : {valor}")

# Definição das Task
task2 = PythonOperator(task_id='tsk2', python_callable=task_read, dag=dag) 

task1 >> task2
