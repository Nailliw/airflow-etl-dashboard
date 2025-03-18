from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import logging
from sqlalchemy import create_engine, text

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuração do banco de dados
DB_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432",
}

def create_table_if_not_exists():
    """Cria a tabela se ela não existir"""
    try:
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS latin_america_deaths (
            country VARCHAR(3),
            year DATE,
            deaths_per_1000 DECIMAL(10,2),
            PRIMARY KEY (country, year)
        )
        """
        
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()
            
        logging.info("Tabela latin_america_deaths criada ou já existente")
        
    except Exception as e:
        logging.error(f"Erro ao criar tabela: {str(e)}")
        raise

def extract_latin_america_deaths_data():
    """Extrai dados de mortes na América Latina do Banco Mundial"""
    try:
        logging.info("Iniciando extração de dados de mortes na América Latina")
        
        # Lista de países da América Latina
        latin_america_countries = [
            'ARG', 'BOL', 'BRA', 'CHL', 'COL', 'CRI', 'CUB', 'DOM', 'ECU', 'SLV',
            'GTM', 'HND', 'MEX', 'NIC', 'PAN', 'PRY', 'PER', 'URY', 'VEN'
        ]
        
        # Extrair dados do Banco Mundial
        base_url = "http://api.worldbank.org/v2/country/{}/indicator/SP.DYN.CDRT.IN?format=json&date=2015:2022"
        all_data = []
        
        for country in latin_america_countries:
            url = base_url.format(country)
            logging.info(f"Buscando dados para o país {country}")
            
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if len(data) > 1 and data[1]:
                    for item in data[1]:
                        all_data.append({
                            'country': country,
                            'year': item['date'],
                            'deaths_per_1000': item['value']
                        })
            else:
                logging.warning(f"Erro ao buscar dados para {country}: {response.status_code}")
        
        df = pd.DataFrame(all_data)
        logging.info(f"Dados extraídos com sucesso. {len(df)} registros.")
        
        # Salvar dados em um arquivo temporário
        df.to_csv('/tmp/latin_america_deaths.csv', index=False)
        logging.info("Dados salvos em /tmp/latin_america_deaths.csv")
        
    except Exception as e:
        logging.error(f"Erro na extração de dados: {str(e)}")
        raise

def transform_latin_america_deaths_data():
    """Transforma os dados de mortes na América Latina"""
    try:
        logging.info("Iniciando transformação dos dados")
        
        # Ler dados do arquivo temporário
        df = pd.read_csv('/tmp/latin_america_deaths.csv')
        
        # Transformar dados
        df['year'] = pd.to_datetime(df['year'] + '-01-01')
        df = df.fillna(0)
        df['deaths_per_1000'] = pd.to_numeric(df['deaths_per_1000'], errors='coerce')
        
        # Salvar dados transformados
        df.to_csv('/tmp/latin_america_deaths_transformed.csv', index=False)
        logging.info("Dados transformados e salvos em /tmp/latin_america_deaths_transformed.csv")
        
    except Exception as e:
        logging.error(f"Erro na transformação dos dados: {str(e)}")
        raise

def load_latin_america_deaths_data():
    """Carrega os dados de mortes na América Latina no banco de dados"""
    try:
        logging.info("Iniciando carregamento dos dados no banco")
        
        # Ler dados transformados
        df = pd.read_csv('/tmp/latin_america_deaths_transformed.csv')
        
        # Carregar no banco
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        # Carregar dados
        df.to_sql('latin_america_deaths', engine, if_exists='replace', index=False)
        logging.info("Dados carregados com sucesso no banco de dados")
        
    except Exception as e:
        logging.error(f"Erro no carregamento dos dados: {str(e)}")
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

dag = DAG(
    'latin_america_deaths_etl',
    default_args=default_args,
    description='ETL de dados de mortes anuais na América Latina',
    schedule_interval='@yearly',  # Executa uma vez por ano
    catchup=False,
    tags=['deaths', 'latin_america'],
)

# Definição das tasks
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table_if_not_exists,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_latin_america_deaths_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_latin_america_deaths_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_latin_america_deaths_data,
    dag=dag,
)

# Definição das dependências
create_table_task >> extract_task >> transform_task >> load_task 