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

def get_crypto_data(time_range):
    """Busca dados das criptomoedas do banco de dados"""
    conn = psycopg2.connect(**DB_CONN)
    
    # Definir período baseado no filtro
    if time_range == "Últimas 24 horas":
        interval = "24 hours"
    elif time_range == "Última semana":
        interval = "7 days"
    elif time_range == "Último mês":
        interval = "30 days"
    else:  # Desde 2025
        interval = "1 year"
    
    query = f"""
        SELECT * FROM crypto_data 
        WHERE timestamp >= NOW() - INTERVAL '{interval}'
        ORDER BY timestamp ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Configuração da página
st.set_page_config(
    page_title="Criptomoedas Dashboard",
    page_icon="📈",
    layout="wide"
)

# Título
st.title("Dashboard Criptomoedas")

# Filtros
st.sidebar.header("Filtros")
time_range = st.sidebar.selectbox(
    "Período dos Dados",
    ["Últimas 24 horas", "Última semana", "Último mês", "Desde 2025"]
)

# Obter dados
crypto_df = get_crypto_data(time_range)

if not crypto_df.empty:
    # Gráfico de preços
    fig_price = px.line(
        crypto_df,
        x='timestamp',
        y='price_usd',
        color='name',
        title=f'Preço das Criptomoedas ({time_range})',
        labels={'timestamp': 'Data/Hora', 'price_usd': 'Preço (USD)', 'name': 'Criptomoeda'}
    )
    fig_price.update_layout(
        xaxis_title="Data/Hora",
        yaxis_title="Preço (USD)",
        hovermode='x unified',
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_price, use_container_width=True)

    # Gráfico de variação de preço
    fig_change = px.bar(
        crypto_df.groupby('name')['price_change_24h'].last().reset_index(),
        x='name',
        y='price_change_24h',
        title='Variação de Preço em 24h (%)',
        labels={'name': 'Criptomoeda', 'price_change_24h': 'Variação (%)'}
    )
    fig_change.update_layout(
        yaxis_title="Variação (%)",
        showlegend=False,
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_change, use_container_width=True)

    # Gráfico de volume
    fig_volume = px.bar(
        crypto_df.groupby('name')['volume_24h'].last().reset_index(),
        x='name',
        y='volume_24h',
        title='Volume de Negociação em 24h (USD)',
        labels={'name': 'Criptomoeda', 'volume_24h': 'Volume (USD)'}
    )
    fig_volume.update_layout(
        yaxis_title="Volume (USD)",
        showlegend=False,
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_volume, use_container_width=True)

    # Gráfico de evolução do volume
    fig_volume_timeline = px.line(
        crypto_df,
        x='timestamp',
        y='volume_24h',
        color='name',
        title=f'Evolução do Volume de Negociação ({time_range})',
        labels={'timestamp': 'Data/Hora', 'volume_24h': 'Volume (USD)', 'name': 'Criptomoeda'}
    )
    fig_volume_timeline.update_layout(
        xaxis_title="Data/Hora",
        yaxis_title="Volume (USD)",
        hovermode='x unified',
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_volume_timeline, use_container_width=True)

    # Tabela de dados
    st.subheader("Últimos Dados")
    st.dataframe(crypto_df)
else:
    st.warning("Nenhum dado disponível. Aguarde o Airflow processar os dados.") 