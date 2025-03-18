from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os
from airflow.exceptions import AirflowException
import logging
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import requests

# Usar variáveis de ambiente para configuração do banco de dados
DB_CONN = {
    "dbname": os.getenv('POSTGRES_DB', 'airflow'),
    "user": os.getenv('POSTGRES_USER', 'airflow'),
    "password": os.getenv('POSTGRES_PASSWORD', 'airflow'),
    "host": 'postgres',
    "port": '5432',
}

def create_table_if_not_exists():
    """Cria a tabela covid_data se ela não existir"""
    try:
        logging.info("Iniciando criação da tabela covid_data")
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS covid_data (
            date DATE,
            country VARCHAR(50),
            total_cases INTEGER,
            new_cases INTEGER,
            total_deaths INTEGER,
            new_deaths INTEGER,
            PRIMARY KEY (date, country)
        );
        """
        
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()
            
        logging.info("Tabela covid_data criada ou já existente")
        
    except Exception as e:
        logging.error(f"Erro ao criar tabela: {str(e)}")
        logging.error(f"Tipo do erro: {type(e).__name__}")
        import traceback
        logging.error(f"Traceback completo: {traceback.format_exc()}")
        raise

def extract_covid_data():
    """Extrai dados de COVID-19 da API do Our World in Data"""
    try:
        logging.info("Iniciando extração de dados de COVID-19")
        
        # URL da API
        url = "https://covid.ourworldindata.org/data/owid-covid-data.json"
        logging.info(f"URL da API: {url}")
        
        response = requests.get(url)
        logging.info(f"Status code da resposta: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logging.info("Dados recebidos com sucesso")
            
            try:
                # Lista para armazenar os dados
                all_data = []
                
                # Processar dados para cada país
                for country_code, country_data in data.items():
                    logging.info(f"Processando dados para o país: {country_code}")
                    
                    if 'data' in country_data:
                        for day_data in country_data['data']:
                            all_data.append({
                                'date': day_data.get('date'),
                                'country': country_code,
                                'total_cases': day_data.get('total_cases', 0),
                                'new_cases': day_data.get('new_cases', 0),
                                'total_deaths': day_data.get('total_deaths', 0),
                                'new_deaths': day_data.get('new_deaths', 0)
                            })
                
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

def transform_covid_data(**context):
    """Transforma os dados de COVID-19"""
    try:
        logging.info("Iniciando transformação dos dados de COVID-19")
        covid_df = context['task_instance'].xcom_pull(task_ids='extract_covid_data')
        
        if covid_df is None or covid_df.empty:
            logging.error("Nenhum dado recebido para transformação")
            raise Exception("Nenhum dado recebido para transformação")
        
        # Converte a coluna de data para datetime
        covid_df['date'] = pd.to_datetime(covid_df['date'])
        logging.info("Coluna date convertida para datetime")
        
        # Preenche valores nulos com 0
        covid_df = covid_df.fillna(0)
        logging.info("Valores nulos preenchidos com 0")
        
        # Converte colunas numéricas para inteiro
        numeric_columns = ['total_cases', 'new_cases', 'total_deaths', 'new_deaths']
        covid_df[numeric_columns] = covid_df[numeric_columns].astype(int)
        logging.info("Colunas numéricas convertidas para inteiro")
        
        logging.info(f"Transformação concluída. DataFrame final com {len(covid_df)} registros")
        return covid_df
    
    except Exception as e:
        logging.error(f"Erro ao transformar dados: {str(e)}")
        logging.error(f"Tipo do erro: {type(e).__name__}")
        import traceback
        logging.error(f"Traceback completo: {traceback.format_exc()}")
        raise

def load_covid_data(**context):
    """Carrega os dados de COVID-19 no banco de dados"""
    try:
        logging.info("Iniciando carregamento dos dados de COVID-19")
        covid_df = context['task_instance'].xcom_pull(task_ids='transform_covid_data')
        
        if covid_df is None or covid_df.empty:
            logging.error("Nenhum dado recebido para carregamento")
            raise Exception("Nenhum dado recebido para carregamento")
        
        # Cria conexão com o banco de dados
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        logging.info("Conexão com o banco de dados estabelecida")
        
        # Carrega os dados na tabela
        covid_df.to_sql('covid_data', engine, if_exists='replace', index=False)
        logging.info(f"Dados carregados com sucesso. {len(covid_df)} registros inseridos")
        
    except Exception as e:
        logging.error(f"Erro ao carregar dados: {str(e)}")
        logging.error(f"Tipo do erro: {type(e).__name__}")
        import traceback
        logging.error(f"Traceback completo: {traceback.format_exc()}")
        raise

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
    task_id="extract_covid_data",
    python_callable=extract_covid_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_covid_data",
    python_callable=transform_covid_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_covid_data",
    python_callable=load_covid_data,
    provide_context=True,
    dag=dag
)

# Definir ordem das tarefas
extract_task >> transform_task >> load_task
