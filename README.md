# 📊 Pipeline de ETL com Apache Airflow e Dashboard Streamlit

Este projeto implementa um **pipeline completo de ETL** usando **Apache Airflow** para extração e carga de dados, **PostgreSQL** como banco de dados e **Streamlit** para visualização interativa. Todo o projeto é **containerizado com Docker**.

## 🏗 Arquitetura do Projeto

1. **Airflow** baixa dados de COVID-19 e insere no PostgreSQL.
2. **PostgreSQL** armazena os dados processados.
3. **Streamlit** lê os dados e exibe gráficos interativos.
4. Tudo orquestrado com **Docker Compose**.

## 📂 Estrutura do Projeto
```
airflow_streamlit_pipeline/
│── airflow/                # Configuração do Apache Airflow
│   ├── dags/               # DAGs do Airflow
│   │   ├── etl_covid.py    # DAG de ETL
│   ├── Dockerfile          # Dockerfile do Airflow
│── streamlit/              # App de visualização
│   ├── app.py              # Código do Streamlit
│   ├── Dockerfile          # Dockerfile do Streamlit
│── docker-compose.yml      # Configuração do Docker Compose
│── .env                    # Variáveis de ambiente
```

## 🚀 Como Executar
### 1️⃣ Clonar o repositório
```bash
git clone https://github.com/seu-usuario/airflow_streamlit_pipeline.git
cd airflow_streamlit_pipeline
```

### 2️⃣ Subir os containers
```bash
docker-compose up -d
```

### 3️⃣ Acessar os serviços
- **Airflow**: [http://localhost:8080](http://localhost:8080)
- **Streamlit Dashboard**: [http://localhost:8501](http://localhost:8501)

## 🛠 Tecnologias Utilizadas
- **Apache Airflow**: Orquestração de ETL
- **PostgreSQL**: Banco de dados
- **Streamlit**: Dashboard interativo
- **Docker & Docker Compose**: Containerização
