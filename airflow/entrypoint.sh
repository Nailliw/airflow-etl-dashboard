#!/bin/bash

# Esperar o PostgreSQL estar pronto
while ! pg_isready -h postgres -p 5432 -U airflow
do
  echo "Waiting for postgres..."
  sleep 2
done
echo "PostgreSQL is ready!"

# Inicializar o banco de dados
airflow db init

# Criar usuário admin
airflow users create \
    --username airflow \
    --firstname airflow \
    --lastname airflow \
    --email airflow@airflow.com \
    --role Admin \
    --password airflow

# Executar o comando passado como argumento
case "$1" in
  webserver)
    exec airflow webserver
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  *)
    exec "$@"
    ;;
esac 