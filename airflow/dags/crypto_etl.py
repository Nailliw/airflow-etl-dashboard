from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os
import requests
from airflow.exceptions import AirflowException

# Usar variáveis de ambiente para configuração do banco de dados
DB_CONN = {
    "dbname": os.getenv('POSTGRES_DB', 'airflow'),
    "user": os.getenv('POSTGRES_USER', 'airflow'),
    "password": os.getenv('POSTGRES_PASSWORD', 'airflow'),
    "host": 'postgres',
    "port": '5432',
}

def get_top_cryptos():
    """Retorna as 5 criptomoedas mais caras"""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "price_desc",
        "per_page": 5,
        "page": 1,
        "sparkline": False
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise AirflowException(f"Erro ao obter dados das criptomoedas: {str(e)}")

def extract(**context):
    try:
        # Criar diretório temporário se não existir
        os.makedirs('/opt/airflow/temp', exist_ok=True)
        
        # Obter dados das criptomoedas
        cryptos = get_top_cryptos()
        
        # Criar DataFrame
        df = pd.DataFrame(cryptos)
        
        # Selecionar e renomear colunas
        columns = {
            'id': 'crypto_id',
            'symbol': 'symbol',
            'name': 'name',
            'current_price': 'price_usd',
            'market_cap': 'market_cap_usd',
            'total_volume': 'volume_24h',
            'price_change_percentage_24h': 'price_change_24h'
        }
        df = df[columns.keys()].rename(columns=columns)
        
        # Adicionar timestamp
        df['timestamp'] = datetime.now()
        
        # Converter tipos de dados
        df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
        df['market_cap_usd'] = pd.to_numeric(df['market_cap_usd'], errors='coerce')
        df['volume_24h'] = pd.to_numeric(df['volume_24h'], errors='coerce')
        df['price_change_24h'] = pd.to_numeric(df['price_change_24h'], errors='coerce')
        
        # Salvar arquivo processado
        temp_file = '/opt/airflow/temp/crypto_data.csv'
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
            CREATE TABLE IF NOT EXISTS crypto_data (
                crypto_id TEXT,
                symbol TEXT,
                name TEXT,
                price_usd DECIMAL(20,8),
                market_cap_usd DECIMAL(30,2),
                volume_24h DECIMAL(30,2),
                price_change_24h DECIMAL(10,2),
                timestamp TIMESTAMP
            )
        """)
        
        # Carregar dados do CSV
        with open(temp_file, 'r') as f:
            next(f)  # Pular cabeçalho
            cur.copy_from(
                f,
                'crypto_data',
                sep=',',
                columns=('crypto_id', 'symbol', 'name', 'price_usd', 
                        'market_cap_usd', 'volume_24h', 'price_change_24h', 'timestamp')
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
    dag_id="crypto_etl",
    start_date=datetime(2024, 3, 18),
    schedule_interval="*/1 * * * *",  # Executar a cada 1 minuto
    catchup=False,
    tags=['crypto', 'etl']
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