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

def get_latin_america_deaths_data():
    """Busca dados de mortes na América Latina do banco de dados"""
    try:
        engine = create_engine(f"postgresql://{DB_CONN['user']}:{DB_CONN['password']}@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}")
        
        query = """
        SELECT country, year, deaths_per_1000
        FROM latin_america_deaths
        ORDER BY country, year
        """
        
        df = pd.read_sql(query, engine)
        return df
        
    except Exception as e:
        st.error(f"Erro ao buscar dados: {str(e)}")
        return pd.DataFrame()

# Configuração da página
st.set_page_config(
    page_title="Mortes na América Latina",
    page_icon="📊",
    layout="wide"
)

# Título e descrição
st.title("📊 Mortes na América Latina")
st.markdown("""
Esta página mostra dados de mortalidade anual para países da América Latina.
Os dados são atualizados anualmente através do ETL `latin_america_deaths_etl`.
""")

# Sidebar
st.sidebar.header("Filtros")

# Filtro de países
df = get_latin_america_deaths_data()
if not df.empty:
    countries = sorted(df['country'].unique())
    selected_countries = st.sidebar.multiselect(
        "Selecione os países",
        options=countries,
        default=countries[:5]  # Primeiros 5 países por padrão
    )
    
    # Filtro de período
    min_year = df['year'].min().year
    max_year = df['year'].max().year
    year_range = st.sidebar.slider(
        "Selecione o período",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year)
    )
    
    # Filtrar dados
    mask = (df['country'].isin(selected_countries)) & \
           (df['year'].dt.year >= year_range[0]) & \
           (df['year'].dt.year <= year_range[1])
    filtered_df = df[mask]
    
    # Gráfico de linha - Evolução da taxa de mortalidade
    st.subheader("Evolução da Taxa de Mortalidade")
    fig_line = px.line(
        filtered_df,
        x='year',
        y='deaths_per_1000',
        color='country',
        title='Taxa de Mortalidade por 1000 Habitantes',
        labels={
            'year': 'Ano',
            'deaths_per_1000': 'Mortes por 1000 Habitantes',
            'country': 'País'
        }
    )
    st.plotly_chart(fig_line, use_container_width=True)
    
    # Gráfico de barras - Comparação entre países
    st.subheader("Comparação entre Países")
    fig_bar = px.bar(
        filtered_df,
        x='country',
        y='deaths_per_1000',
        color='year',
        title='Taxa de Mortalidade por País',
        labels={
            'country': 'País',
            'deaths_per_1000': 'Mortes por 1000 Habitantes',
            'year': 'Ano'
        }
    )
    st.plotly_chart(fig_bar, use_container_width=True)
    
    # Mapa de calor
    st.subheader("Mapa de Calor - Taxa de Mortalidade")
    pivot_df = filtered_df.pivot(index='country', columns='year', values='deaths_per_1000')
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='Reds'
    ))
    fig_heatmap.update_layout(
        title='Mapa de Calor - Taxa de Mortalidade por País e Ano',
        xaxis_title='Ano',
        yaxis_title='País'
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)
    
    # Tabela de dados
    st.subheader("Dados Detalhados")
    st.dataframe(filtered_df)
    
else:
    st.warning("Não há dados disponíveis. Aguarde a execução do ETL para visualizar as informações.") 