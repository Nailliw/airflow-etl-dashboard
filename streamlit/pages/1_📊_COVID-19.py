import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os

# Configuração do banco de dados
DB_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432",
}

def get_covid_data():
    """Busca dados de COVID-19 do banco de dados"""
    try:
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        query = """
        SELECT date, country, total_cases, new_cases, total_deaths, new_deaths
        FROM covid_data
        ORDER BY date DESC
        LIMIT 1000
        """
        
        df = pd.read_sql(query, engine)
        return df
        
    except Exception as e:
        st.error(f"Erro ao buscar dados: {str(e)}")
        return pd.DataFrame()

# Configuração da página
st.set_page_config(
    page_title="COVID-19 Dashboard",
    page_icon="🦠",
    layout="wide"
)

# Título e descrição
st.title("🦠 COVID-19 Dashboard")
st.markdown("""
Esta página mostra dados de casos e mortes por COVID-19 em diferentes países.
Os dados são atualizados diariamente através do ETL `etl_covid`.
""")

# Sidebar
st.sidebar.header("Filtros")

# Filtro de período
period = st.sidebar.selectbox(
    "Selecione o período",
    ["Últimas 24 horas", "Última semana", "Último mês"]
)

# Filtro de países
df = get_covid_data()
if not df.empty:
    countries = sorted(df['country'].unique())
    selected_countries = st.sidebar.multiselect(
        "Selecione os países",
        options=countries,
        default=countries[:5]  # Primeiros 5 países por padrão
    )
    
    # Filtrar dados
    mask = df['country'].isin(selected_countries)
    filtered_df = df[mask]
    
    # Gráfico de barras - Total de casos por país
    st.subheader("Total de Casos por País")
    fig_cases = px.bar(
        filtered_df.groupby('country')['total_cases'].last().reset_index(),
        x='country',
        y='total_cases',
        title='Total de Casos de COVID-19 por País',
        labels={
            'country': 'País',
            'total_cases': 'Total de Casos'
        }
    )
    st.plotly_chart(fig_cases, use_container_width=True)
    
    # Gráfico de linha - Evolução de casos
    st.subheader("Evolução de Casos")
    fig_evolution = px.line(
        filtered_df,
        x='date',
        y='total_cases',
        color='country',
        title='Evolução do Total de Casos',
        labels={
            'date': 'Data',
            'total_cases': 'Total de Casos',
            'country': 'País'
        }
    )
    st.plotly_chart(fig_evolution, use_container_width=True)
    
    # Gráfico de barras - Novos casos
    st.subheader("Novos Casos por País")
    fig_new_cases = px.bar(
        filtered_df.groupby('country')['new_cases'].last().reset_index(),
        x='country',
        y='new_cases',
        title='Novos Casos de COVID-19 por País',
        labels={
            'country': 'País',
            'new_cases': 'Novos Casos'
        }
    )
    st.plotly_chart(fig_new_cases, use_container_width=True)
    
    # Tabela de dados
    st.subheader("Dados Detalhados")
    st.dataframe(filtered_df)
    
else:
    st.warning("Não há dados disponíveis. Aguarde a execução do ETL para visualizar as informações.") 