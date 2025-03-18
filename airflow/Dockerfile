FROM apache/airflow:2.7.3-python3.8

USER root
USER airflow
RUN pip install pandas requests psycopg2-binary


COPY dags/ /opt/airflow/dags/
COPY airflow.cfg /opt/airflow/airflow.cfg

COPY entrypoint.sh /opt/airflow/entrypoint.sh

USER root
# Garantir que o script tem as permissões corretas
RUN chmod +x /opt/airflow/entrypoint.sh && \
    chown airflow:root /opt/airflow/entrypoint.sh

USER airflow
ENTRYPOINT ["/bin/bash", "/opt/airflow/entrypoint.sh"]
