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
    
    # Buscar dados de 2018 até 2020
    query = """
        SELECT * FROM covid_data 
        WHERE date >= '2018-01-01' AND date <= '2020-12-31'
        ORDER BY date DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_deaths_br_data():
    """Busca dados de mortes no Brasil"""
    conn = psycopg2.connect(**DB_CONN)
    query = "SELECT * FROM deaths_br ORDER BY date DESC"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Configuração da página
st.set_page_config(
    page_title="Dashboard de Mortes",
    page_icon="💀",
    layout="wide"
)

# Título
st.title("Dashboard de Mortes")

# Filtros
st.sidebar.header("Filtros")
time_range = st.sidebar.selectbox(
    "Período dos Dados",
    ["2018-2020"]  # Removido outros períodos já que agora só mostramos 2018-2020
)

# Obter dados
covid_df = get_covid_data(time_range)
deaths_br_df = get_deaths_br_data()

# Seção de Mortes por COVID-19
st.header("Mortes por COVID-19 (2018-2020)")

if not covid_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico de mortes totais por país
        fig_deaths = px.bar(
            covid_df.groupby('country')['total_deaths'].last().reset_index(),
            x='country',
            y='total_deaths',
            title='Total de Mortes por COVID-19 por País (2018-2020)',
            labels={'country': 'País', 'total_deaths': 'Total de Mortes'}
        )
        fig_deaths.update_layout(
            yaxis_title="Total de Mortes",
            showlegend=False,
            xaxis_tickangle=45
        )
        st.plotly_chart(fig_deaths, use_container_width=True)

    with col2:
        # Gráfico de novos casos por país
        fig_new = px.bar(
            covid_df.groupby('country')['new_cases'].last().reset_index(),
            x='country',
            y='new_cases',
            title='Novos Casos de COVID-19 por País (2018-2020)',
            labels={'country': 'País', 'new_cases': 'Novos Casos'}
        )
        fig_new.update_layout(
            yaxis_title="Novos Casos",
            showlegend=False,
            xaxis_tickangle=45
        )
        st.plotly_chart(fig_new, use_container_width=True)

    # Gráfico de evolução temporal das mortes
    fig_timeline = px.line(
        covid_df,
        x='date',
        y='total_deaths',
        color='country',
        title='Evolução Temporal das Mortes por COVID-19 (2018-2020)',
        labels={'date': 'Data', 'total_deaths': 'Total de Mortes', 'country': 'País'}
    )
    fig_timeline.update_layout(
        xaxis_title="Data",
        yaxis_title="Total de Mortes",
        hovermode='x unified'
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

    # Tabela de dados de COVID-19
    st.subheader("Dados de Mortes por COVID-19 (2018-2020)")
    st.dataframe(covid_df)
else:
    st.warning("Nenhum dado de COVID-19 disponível. Aguarde o Airflow processar os dados.")

# Seção de Mortes Gerais no Brasil
st.header("Mortes Gerais no Brasil")

if not deaths_br_df.empty:
    # Gráfico de evolução de mortes totais
    fig_total = px.line(
        deaths_br_df[deaths_br_df['cause'] == 'Total'],
        x='date',
        y='deaths',
        title='Evolução de Mortes Totais no Brasil',
        labels={'date': 'Data', 'deaths': 'Total de Mortes'}
    )
    fig_total.update_layout(
        xaxis_title="Data",
        yaxis_title="Total de Mortes",
        hovermode='x unified'
    )
    st.plotly_chart(fig_total, use_container_width=True)

    # Gráfico de distribuição de mortes por causa
    fig_causes = px.bar(
        deaths_br_df[deaths_br_df['cause'] != 'Total'].groupby('cause')['deaths'].sum().reset_index(),
        x='cause',
        y='deaths',
        title='Distribuição de Mortes por Causa no Brasil',
        labels={'cause': 'Causa', 'deaths': 'Total de Mortes'}
    )
    fig_causes.update_layout(
        xaxis_title="Causa",
        yaxis_title="Total de Mortes",
        showlegend=False,
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_causes, use_container_width=True)

    # Gráfico de evolução das mortes por causa
    fig_evolution = px.line(
        deaths_br_df[deaths_br_df['cause'] != 'Total'],
        x='date',
        y='deaths',
        color='cause',
        title='Evolução das Mortes por Causa no Brasil',
        labels={'date': 'Data', 'deaths': 'Número de Mortes', 'cause': 'Causa'}
    )
    fig_evolution.update_layout(
        xaxis_title="Data",
        yaxis_title="Número de Mortes",
        hovermode='x unified'
    )
    st.plotly_chart(fig_evolution, use_container_width=True)

    # Tabela de dados de mortes gerais
    st.subheader("Dados de Mortes Gerais no Brasil")
    st.dataframe(deaths_br_df)
else:
    st.warning("Nenhum dado de mortes gerais disponível. Aguarde o Airflow processar os dados.") 