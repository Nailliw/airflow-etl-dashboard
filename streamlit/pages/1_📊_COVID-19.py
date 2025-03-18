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

def get_covid_data(time_range):
    """Busca dados de COVID-19 do banco de dados"""
    conn = psycopg2.connect(**DB_CONN)
    
    # Definir período baseado no filtro
    if time_range == "Últimas 24 horas":
        interval = "24 hours"
    elif time_range == "Última semana":
        interval = "7 days"
    elif time_range == "Último mês":
        interval = "30 days"
    else:  # Desde 2018
        interval = "6 years"
    
    query = f"""
        SELECT * FROM covid_data 
        WHERE date >= NOW() - INTERVAL '{interval}'
        ORDER BY date DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Configuração da página
st.set_page_config(
    page_title="COVID-19 Dashboard",
    page_icon="📊",
    layout="wide"
)

# Título
st.title("Dashboard COVID-19")

# Filtros
st.sidebar.header("Filtros")
time_range = st.sidebar.selectbox(
    "Período dos Dados",
    ["Últimas 24 horas", "Última semana", "Último mês", "Desde 2018"]
)

# Obter dados
covid_df = get_covid_data(time_range)

if not covid_df.empty:
    # Gráfico de casos totais por país
    fig_total = px.bar(
        covid_df.groupby('country')['total_cases'].last().reset_index(),
        x='country',
        y='total_cases',
        title=f'Total de Casos por País ({time_range})',
        labels={'country': 'País', 'total_cases': 'Total de Casos'}
    )
    fig_total.update_layout(
        yaxis_title="Total de Casos",
        showlegend=False,
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_total, use_container_width=True)

    # Gráfico de novos casos por país
    fig_new = px.bar(
        covid_df.groupby('country')['new_cases'].last().reset_index(),
        x='country',
        y='new_cases',
        title=f'Novos Casos por País ({time_range})',
        labels={'country': 'País', 'new_cases': 'Novos Casos'}
    )
    fig_new.update_layout(
        yaxis_title="Novos Casos",
        showlegend=False,
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_new, use_container_width=True)

    # Gráfico de mortes totais por país
    fig_deaths = px.bar(
        covid_df.groupby('country')['total_deaths'].last().reset_index(),
        x='country',
        y='total_deaths',
        title=f'Total de Mortes por País ({time_range})',
        labels={'country': 'País', 'total_deaths': 'Total de Mortes'}
    )
    fig_deaths.update_layout(
        yaxis_title="Total de Mortes",
        showlegend=False,
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_deaths, use_container_width=True)

    # Gráfico de evolução temporal dos casos
    fig_timeline = px.line(
        covid_df,
        x='date',
        y='total_cases',
        color='country',
        title=f'Evolução Temporal dos Casos ({time_range})',
        labels={'date': 'Data', 'total_cases': 'Total de Casos', 'country': 'País'}
    )
    fig_timeline.update_layout(
        xaxis_title="Data",
        yaxis_title="Total de Casos",
        hovermode='x unified'
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

    # Tabela de dados
    st.subheader("Últimos Dados")
    st.dataframe(covid_df)
else:
    st.warning("Nenhum dado disponível. Aguarde o Airflow processar os dados.") 