from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import Variable

# Definição da DAG
dag = DAG('variaveis', description='dag variaveis',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)

def print_variable(**context):
    minha_variavel = Variable.get('minha_var')
    print(f'o valor da variável é: {minha_variavel}')


# Definição das Task1
task1 = PythonOperator(task_id='tsk1', python_callable=print_variable, dag=dag) 

task1