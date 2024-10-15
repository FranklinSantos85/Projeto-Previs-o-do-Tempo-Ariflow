from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import statistics as sts

# Definição da DAG
dag = DAG('pythonoperator', description='dag pythonoperator',
    schedule_interval=None, start_date=datetime(2024, 9, 28),
    catchup=False)

def data_cleaner():
    # Lendo o dataset
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.columns = ['Id', 'Score', 'Estado', 'Genero', 'Idade', 'Patrimonio', 
                       'Saldo', 'Produto', 'TemCartCredito', 'Ativo', 'Salario', 'Saiu']
    
    mediana = sts.median(dataset['Salario'])
    dataset['Salario'].fillna(mediana, inplace=True)
    
    # Preenchendo valores ausentes na coluna Genero com "Masculino"
    dataset['Genero'].fillna('Masculino', inplace=True)

     # Substituindo 'M' por 'Masculino' e 'F' por 'Feminino'
    dataset['Genero'].replace({'M': 'Masculino', 'F': 'Feminino'}, inplace=True)
    
    # Tratamento de Idade: substituindo valores fora do intervalo (0-120) pela mediana
    mediana = sts.median(dataset['Idade'])
    dataset.loc[(dataset['Idade']< 0) | (dataset['Idade']> 120), 'Idade'] = mediana
    
    # Removendo duplicatas baseadas no campo 'Id'
    dataset.drop_duplicates(subset='Id', keep='first', inplace=True)

    # Exportando o dataset limpo para um novo arquivo CSV
    dataset.to_csv('/opt/airflow/data/Churn_Clean.csv', sep=';', index=False)

# Definindo a task
t1 = PythonOperator(task_id='t1', python_callable=data_cleaner, dag=dag)

t1

