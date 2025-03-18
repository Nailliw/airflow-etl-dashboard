# Airflow ETL Dashboard

## 📌 Visão Geral
Este projeto implementa um pipeline ETL utilizando **Apache Airflow** para extração, transformação e carga de dados, armazenando as informações em um banco de dados PostgreSQL. Além disso, um **dashboard interativo** foi desenvolvido com **Streamlit** para visualizar os dados.

## 🚀 Tecnologias Utilizadas
- **Apache Airflow**: Orquestração do pipeline ETL
- **PostgreSQL**: Armazenamento dos dados
- **Docker**: Contêiner para PostgreSQL
- **Pandas**: Manipulação e transformação dos dados
- **Requests**: Coleta de dados de API pública
- **Streamlit**: Dashboard interativo para visualização dos dados

## 🏗 Estrutura do Projeto
```
airflow-etl-dashboard/
│── dags/
│   ├── etl_pipeline.py  # DAG do Airflow
│── app/
│   ├── dashboard.py  # Dashboard Streamlit
│── docker/
│   ├── docker-compose.yml  # Configuração do Docker para PostgreSQL
│── requirements.txt  # Dependências
│── README.md  # Documentação do projeto
```

## ⚙️ Configuração e Execução

### 1️⃣ Configurar o Banco de Dados com Docker
Para rodar um contêiner PostgreSQL, crie um arquivo `docker-compose.yml` na pasta `docker/` e adicione:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:latest
    container_name: postgres_db
    restart: always
    environment:
      POSTGRES_USER: meu_user
      POSTGRES_PASSWORD: minha_senha
      POSTGRES_DB: meu_db
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

Agora, inicie o contêiner com:
```sh
cd docker/
docker-compose up -d
```

### 2️⃣ Configurar o Airflow
Se ainda não configurou o Airflow, inicialize o banco de dados:
```sh
airflow db init
```
Inicie os serviços do Airflow:
```sh
airflow scheduler &
airflow webserver --port 8080 &
```
Adicione a DAG ao diretório do Airflow e confirme a listagem:
```sh
mv dags/etl_pipeline.py ~/airflow/dags/
airflow dags list
```
Acesse a interface do Airflow em `http://localhost:8080` e ative a DAG `etl_exchange_rates`.

### 3️⃣ Executar o Dashboard
Após a execução do ETL, inicie o dashboard Streamlit:
```sh
cd app/
streamlit run dashboard.py
```
Acesse o dashboard em `http://localhost:8501`.

## 📊 Fluxo do Pipeline
1. **Extração**: Coleta de dados da API de taxas de câmbio.
2. **Transformação**: Limpeza e ajuste dos valores.
3. **Carga**: Armazenamento dos dados no PostgreSQL.
4. **Visualização**: Dashboard interativo para análise.

## 🔥 Contribuição
Sinta-se à vontade para contribuir com melhorias no projeto! Basta clonar o repositório, criar um branch e abrir um pull request.

## 📜 Licença
Este projeto está sob a licença MIT. Sinta-se livre para utilizá-lo e modificá-lo conforme necessário.

