from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os
from airflow.exceptions import AirflowException

# Usar variáveis de ambiente para configuração do banco de dados
DB_CONN = {
    "dbname": os.getenv('POSTGRES_DB', 'airflow'),
    "user": os.getenv('POSTGRES_USER', 'airflow'),
    "password": os.getenv('POSTGRES_PASSWORD', 'airflow'),
    "host": 'postgres',
    "port": '5432',
}

def extract(**context):
    try:
        # Criar diretório temporário se não existir
        os.makedirs('/opt/airflow/temp', exist_ok=True)
        
        # Download dos dados
        url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
        df = pd.read_csv(url)
        
        # Selecionar apenas as colunas necessárias
        columns = ['iso_code', 'location', 'date', 'total_cases', 
                  'new_cases', 'total_deaths', 'new_deaths']
        df = df[columns]
        
        # Converter tipos de dados
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Converter colunas numéricas para inteiros
        numeric_columns = ['total_cases', 'new_cases', 'total_deaths', 'new_deaths']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        # Salvar arquivo processado
        temp_file = '/opt/airflow/temp/covid_data.csv'
        df.to_csv(temp_file, index=False)
        
        # Passar o caminho do arquivo para a próxima task
        context['task_instance'].xcom_push(key='temp_file', value=temp_file)
        
    except Exception as e:
        raise AirflowException(f"Erro na extração dos dados: {str(e)}")

def load(**context):
    try:
        # Recuperar caminho do arquivo da task anterior
        temp_file = context['task_instance'].xcom_pull(task_ids='extract', key='temp_file')
        
        # Conectar ao banco de dados
        conn = psycopg2.connect(**DB_CONN)
        cur = conn.cursor()
        
        # Criar tabela se não existir
        cur.execute("""
            CREATE TABLE IF NOT EXISTS covid_data (
                iso_code TEXT,
                location TEXT,
                date DATE,
                total_cases INTEGER,
                new_cases INTEGER,
                total_deaths INTEGER,
                new_deaths INTEGER
            )
        """)
        
        # Limpar dados antigos
        cur.execute("TRUNCATE TABLE covid_data")
        
        # Carregar dados do CSV
        with open(temp_file, 'r') as f:
            next(f)  # Pular cabeçalho
            cur.copy_from(
                f,
                'covid_data',
                sep=',',
                columns=('iso_code', 'location', 'date', 'total_cases', 
                        'new_cases', 'total_deaths', 'new_deaths')
            )
        
        # Commit e fechar conexões
        conn.commit()
        cur.close()
        conn.close()
        
        # Limpar arquivo temporário
        os.remove(temp_file)
        
    except Exception as e:
        if conn:
            conn.rollback()
            cur.close()
            conn.close()
        raise AirflowException(f"Erro no carregamento dos dados: {str(e)}")

# Definição da DAG
dag = DAG(
    dag_id="covid_etl",
    start_date=datetime(2019, 3, 18),
    schedule_interval="@daily",
    catchup=False,
    tags=['covid', 'etl']
)

# Tarefas
extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    provide_context=True,
    dag=dag
)

# Definir ordem das tarefas
extract_task >> load_task
