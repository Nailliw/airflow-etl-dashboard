import pandas as pd
import psycopg2
import os
import requests
from datetime import datetime
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
    "host": "localhost",
    "port": "5432",
}

def test_crypto_etl():
    """Testa o ETL de criptomoedas"""
    try:
        logging.info("Iniciando teste do ETL de criptomoedas")
        
        # Função para obter as criptomoedas mais caras
        def get_top_cryptos():
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "price_desc",
                "per_page": 5,
                "page": 1,
                "sparkline": False
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        
        # Extrair dados
        logging.info("Extraindo dados das criptomoedas")
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
        
        logging.info(f"Dados extraídos com sucesso. {len(df)} registros.")
        
        # Carregar no banco
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        # Criar tabela se não existir
        create_table_query = """
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
        """
        
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()
        
        # Carregar dados
        df.to_sql('crypto_data', engine, if_exists='append', index=False)
        logging.info("Dados carregados com sucesso no banco de dados")
        
    except Exception as e:
        logging.error(f"Erro no teste do ETL de criptomoedas: {str(e)}")
        raise

def test_covid_etl():
    """Testa o ETL de COVID-19"""
    try:
        logging.info("Iniciando teste do ETL de COVID-19")
        
        # Extrair dados
        url = "https://covid.ourworldindata.org/data/owid-covid-data.json"
        logging.info(f"Buscando dados de: {url}")
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Processar dados
        all_data = []
        for country_code, country_data in data.items():
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
        
        df = pd.DataFrame(all_data)
        logging.info(f"Dados extraídos com sucesso. {len(df)} registros.")
        
        # Transformar dados
        df['date'] = pd.to_datetime(df['date'])
        df = df.fillna(0)
        numeric_columns = ['total_cases', 'new_cases', 'total_deaths', 'new_deaths']
        df[numeric_columns] = df[numeric_columns].astype(int)
        
        # Carregar no banco
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        # Criar tabela se não existir
        create_table_query = """
        CREATE TABLE IF NOT EXISTS covid_data (
            date DATE,
            country VARCHAR(50),
            total_cases INTEGER,
            new_cases INTEGER,
            total_deaths INTEGER,
            new_deaths INTEGER,
            PRIMARY KEY (date, country)
        )
        """
        
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()
        
        # Carregar dados
        df.to_sql('covid_data', engine, if_exists='replace', index=False)
        logging.info("Dados carregados com sucesso no banco de dados")
        
    except Exception as e:
        logging.error(f"Erro no teste do ETL de COVID-19: {str(e)}")
        raise

def test_latin_america_deaths_etl():
    """Testa o ETL de mortes anuais na América Latina"""
    try:
        logging.info("Iniciando teste do ETL de mortes na América Latina")
        
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
        logging.info(f"Primeiras linhas dos dados: {df.head()}")
        
        # Transformar dados
        df['year'] = pd.to_datetime(df['year'] + '-01-01')
        df = df.fillna(0)
        df['deaths_per_1000'] = pd.to_numeric(df['deaths_per_1000'], errors='coerce')
        
        # Carregar no banco
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        # Criar tabela se não existir
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
        
        # Carregar dados
        df.to_sql('latin_america_deaths', engine, if_exists='replace', index=False)
        logging.info("Dados carregados com sucesso no banco de dados")
        
    except Exception as e:
        logging.error(f"Erro no teste do ETL de mortes na América Latina: {str(e)}")
        logging.error(f"Tipo do erro: {type(e)}")
        import traceback
        logging.error(f"Stack trace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    try:
        # Testar cada ETL
        logging.info("Iniciando testes dos ETLs")
        
        test_crypto_etl()
        logging.info("ETL de criptomoedas concluído com sucesso")
        
        test_covid_etl()
        logging.info("ETL de COVID-19 concluído com sucesso")
        
        test_latin_america_deaths_etl()
        logging.info("ETL de mortes na América Latina concluído com sucesso")
        
        logging.info("Todos os testes concluídos com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro durante os testes: {str(e)}")
        raise 