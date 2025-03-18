from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
import requests
from io import BytesIO

# Configurações do banco de dados
DB_CONN = {
    "dbname": os.getenv('POSTGRES_DB', 'airflow'),
    "user": os.getenv('POSTGRES_USER', 'airflow'),
    "password": os.getenv('POSTGRES_PASSWORD', 'airflow'),
    "host": 'postgres',
    "port": '5432',
}

def extract_deaths_data():
    """Extrai dados de mortes do Brasil do DATASUS"""
    # URL do dataset de mortalidade do DATASUS
    base_url = "https://servicodados.ibge.gov.br/api/v3/agregados/7358/periodos/{}/variaveis/{}?localidades=N6[all]"
    
    # Período de 2018 até o ano atual
    current_year = datetime.now().year
    period = f"2018-{current_year}"
    
    # Variáveis para buscar
    variables = ["93", "94", "95", "96", "97"]  # Códigos das principais causas de morte
    
    # Dicionário com os nomes das causas de morte
    cause_names = {
        "93": "Doenças do aparelho circulatório",
        "94": "Neoplasias (tumores)",
        "95": "Doenças do aparelho respiratório",
        "96": "Causas externas",
        "97": "Outras causas"
    }
    
    # Lista para armazenar os dados
    all_data = []
    
    # Buscar dados para cada variável
    for var in variables:
        url = base_url.format(period, var)
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            # Processar os dados
            for result in data[0]['resultados'][0]['series']:
                for year_data in result['serie'].items():
                    year = year_data[0]
                    value = year_data[1]
                    
                    all_data.append({
                        'date': f"{year}-01-01",
                        'cause': cause_names[var],
                        'deaths': value
                    })
    
    # Criar DataFrame
    df = pd.DataFrame(all_data)
    
    # Calcular totais por ano
    yearly_totals = df.groupby('date')['deaths'].sum().reset_index()
    yearly_totals['cause'] = 'Total'
    
    # Combinar totais com causas específicas
    df = pd.concat([df, yearly_totals], ignore_index=True)
    
    return df

def transform_deaths_data(**context):
    """Transforma os dados de mortes"""
    deaths_df = context['task_instance'].xcom_pull(task_ids='extract_deaths_data')
    
    # Converte a coluna de data para datetime
    deaths_df['date'] = pd.to_datetime(deaths_df['date'])
    
    # Preenche valores nulos com 0
    deaths_df = deaths_df.fillna(0)
    
    return deaths_df

def load_deaths_data(**context):
    """Carrega os dados de mortes no banco de dados"""
    deaths_df = context['task_instance'].xcom_pull(task_ids='transform_deaths_data')
    
    # Cria conexão com o banco de dados
    engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
    
    # Carrega os dados na tabela
    deaths_df.to_sql('deaths_br', engine, if_exists='replace', index=False)

# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_deaths_br',
    default_args=default_args,
    description='ETL para dados de mortes gerais no Brasil',
    schedule_interval='@daily',
    catchup=False,
    tags=['deaths', 'brazil', 'datasus'],
)

# Tarefas
extract_task = PythonOperator(
    task_id='extract_deaths_data',
    python_callable=extract_deaths_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_deaths_data',
    python_callable=transform_deaths_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_deaths_data',
    python_callable=load_deaths_data,
    dag=dag,
)

# Dependências
extract_task >> transform_task >> load_task 