from airflow import DAG
from airflow.operators.bash import BashOperator  #Import correto no Airflow 2.x
from datetime import datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator #Importamos o módulo "TriggerDagRunOperator" para chamar a DAG externa

# Definição da DAG
dag = DAG('dagrunDAG1', description='dag run dag',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)

# Definição das Task1
task1 = BashOperator(task_id='task1', bash_command='sleep 5', dag=dag) 
task2 = TriggerDagRunOperator(task_id='task2', trigger_dag_id="dagrunDAG2", dag=dag) #Na task2, estamos usando o módulo "TriggerDagRunOperator" para executar a DAG2 que criamos!!!


task1 >> task2
