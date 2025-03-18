import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
import os

# Configuração do banco de dados
DB_CONN = {
    "dbname": os.getenv('POSTGRES_DB', 'airflow'),
    "user": os.getenv('POSTGRES_USER', 'airflow'),
    "password": os.getenv('POSTGRES_PASSWORD', 'airflow'),
    "host": 'postgres',
    "port": '5432',
}

def get_crypto_data():
    """Busca dados das criptomoedas do banco de dados"""
    conn = psycopg2.connect(**DB_CONN)
    query = """
        SELECT * FROM crypto_data 
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY timestamp DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_covid_data():
    """Busca dados do COVID do banco de dados"""
    conn = psycopg2.connect(**DB_CONN)
    query = "SELECT * FROM covid_data ORDER BY date DESC LIMIT 10"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Configuração da página
st.set_page_config(
    page_title="Dashboard ETL",
    page_icon="📊",
    layout="wide"
)

# Título
st.title("Dashboard ETL - COVID e Criptomoedas")

# Criar duas colunas
col1, col2 = st.columns(2)

# Coluna 1 - Dados do COVID
with col1:
    st.header("Dados do COVID-19")
    covid_df = get_covid_data()
    
    # Gráfico de casos totais
    fig_covid = px.bar(
        covid_df,
        x='location',
        y='total_cases',
        title='Casos Totais por País',
        labels={'location': 'País', 'total_cases': 'Casos Totais'}
    )
    st.plotly_chart(fig_covid, use_container_width=True)
    
    # Tabela de dados
    st.subheader("Últimos Dados")
    st.dataframe(covid_df)

# Coluna 2 - Dados das Criptomoedas
with col2:
    st.header("Top 5 Criptomoedas")
    crypto_df = get_crypto_data()
    
    # Gráfico de preços
    fig_crypto = px.line(
        crypto_df,
        x='timestamp',
        y='price_usd',
        color='name',
        title='Preço das Criptomoedas (24h)',
        labels={'timestamp': 'Data/Hora', 'price_usd': 'Preço (USD)', 'name': 'Criptomoeda'}
    )
    st.plotly_chart(fig_crypto, use_container_width=True)
    
    # Tabela de dados
    st.subheader("Últimos Dados")
    st.dataframe(crypto_df)

# Adicionar informações sobre atualização
st.sidebar.title("Informações")
st.sidebar.write("""
### Atualização dos Dados
- **COVID-19**: Atualizado diariamente
- **Criptomoedas**: Atualizado a cada 30 minutos
""")

# Adicionar filtros
st.sidebar.header("Filtros")
time_range = st.sidebar.selectbox(
    "Período dos Dados",
    ["Últimas 24 horas", "Última semana", "Último mês"]
)
