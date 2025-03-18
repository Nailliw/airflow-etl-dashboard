from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine, text
import requests
from io import BytesIO
import logging

# Configuração do banco de dados
DB_CONN = {
    "dbname": os.getenv('POSTGRES_DB', 'airflow'),
    "user": os.getenv('POSTGRES_USER', 'airflow'),
    "password": os.getenv('POSTGRES_PASSWORD', 'airflow'),
    "host": 'postgres',
    "port": '5432',
}

def create_table_if_not_exists():
    """Cria a tabela deaths_br se ela não existir"""
    try:
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS deaths_br (
            date DATE,
            deaths INTEGER,
            country VARCHAR(50) DEFAULT 'Brazil',
            PRIMARY KEY (date)
        );
        """
        
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()
            
        logging.info("Tabela deaths_br criada ou já existente")
        
    except Exception as e:
        logging.error(f"Erro ao criar tabela: {str(e)}")
        raise

def extract_deaths_data():
    """Extrai dados de mortes do Brasil do DATASUS"""
    try:
        logging.info("Iniciando extração de dados de mortes do DATASUS")
        
        # URL do dataset de mortalidade do DATASUS
        base_url = "https://servicodados.ibge.gov.br/api/v3/agregados/7358/periodos/{}/variaveis/{}?localidades=N6[all]"
        
        # Período de 2020 até o ano atual
        current_year = datetime.now().year
        period = f"2020-{current_year}"
        logging.info(f"Período de busca: {period}")
        
        # Variável para buscar (total de mortes)
        var = "93"  # Código para total de mortes
        logging.info("Buscando dados de mortes totais")
        
        url = base_url.format(period, var)
        logging.info(f"URL: {url}")
        
        response = requests.get(url)
        logging.info(f"Status code da resposta: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logging.info("Dados recebidos com sucesso")
            
            try:
                # Lista para armazenar os dados
                all_data = []
                
                # Processar os dados
                for result in data[0]['resultados'][0]['series']:
                    for year_data in result['serie'].items():
                        year = year_data[0]
                        value = year_data[1]
                        
                        all_data.append({
                            'date': f"{year}-01-01",
                            'deaths': value
                        })
                
                logging.info(f"Processados {len(all_data)} anos")
                
                # Criar DataFrame
                df = pd.DataFrame(all_data)
                logging.info(f"DataFrame criado com {len(df)} registros")
                
                if df.empty:
                    logging.error("Nenhum dado foi encontrado")
                    raise Exception("Nenhum dado foi encontrado")
                
                return df
                
            except Exception as e:
                logging.error(f"Erro ao processar dados: {str(e)}")
                logging.error(f"Estrutura dos dados recebidos: {data}")
                raise
        else:
            logging.error(f"Erro ao buscar dados: {response.status_code}")
            logging.error(f"Resposta da API: {response.text}")
            raise Exception(f"Erro na API: {response.status_code}")
    
    except Exception as e:
        logging.error(f"Erro ao extrair dados: {str(e)}")
        logging.error(f"Tipo do erro: {type(e).__name__}")
        import traceback
        logging.error(f"Traceback completo: {traceback.format_exc()}")
        raise

def transform_deaths_data(**context):
    """Transforma os dados de mortes"""
    try:
        deaths_df = context['task_instance'].xcom_pull(task_ids='extract_deaths_data')
        
        if deaths_df is None or deaths_df.empty:
            raise Exception("Nenhum dado recebido para transformação")
        
        # Converte a coluna de data para datetime
        deaths_df['date'] = pd.to_datetime(deaths_df['date'])
        
        # Preenche valores nulos com 0
        deaths_df = deaths_df.fillna(0)
        
        # Adiciona coluna de país
        deaths_df['country'] = 'Brazil'
        
        return deaths_df
    
    except Exception as e:
        logging.error(f"Erro ao transformar dados: {str(e)}")
        raise

def load_deaths_data(**context):
    """Carrega os dados de mortes no banco de dados"""
    try:
        deaths_df = context['task_instance'].xcom_pull(task_ids='transform_deaths_data')
        
        if deaths_df is None or deaths_df.empty:
            raise Exception("Nenhum dado recebido para carregamento")
        
        # Cria conexão com o banco de dados
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        # Carrega os dados na tabela
        deaths_df.to_sql('deaths_br', engine, if_exists='replace', index=False)
        
    except Exception as e:
        logging.error(f"Erro ao carregar dados: {str(e)}")
        raise

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

# dag = DAG(
#     'etl_deaths_br',
#     default_args=default_args,
#     description='ETL para dados de mortes gerais no Brasil',
#     schedule_interval='@daily',
#     catchup=False,
#     tags=['deaths', 'brazil', 'datasus'],
# )

# # Tarefas
# create_table_task = PythonOperator(
#     task_id='create_table',
#     python_callable=create_table_if_not_exists,
#     dag=dag,
# )

# extract_task = PythonOperator(
#     task_id='extract_deaths_data',
#     python_callable=extract_deaths_data,
#     dag=dag,
# )

# transform_task = PythonOperator(
#     task_id='transform_deaths_data',
#     python_callable=transform_deaths_data,
#     dag=dag,
# )

# load_task = PythonOperator(
#     task_id='load_deaths_data',
#     python_callable=load_deaths_data,
#     dag=dag,
# )

# # Dependências
# create_table_task >> extract_task >> transform_task >> load_task 