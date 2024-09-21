FROM python:3.9-slim

# Instalar dependencias necesarias
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean

# Instalar librerías Python
RUN pip install --no-cache-dir apache-airflow pandas psycopg2-binary requests

# Copiar el script y el entrypoint
COPY dags/prin.py /usr/local/airflow/dags/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

# Definir el entrypoint para iniciar Airflow
ENTRYPOINT ["/entrypoint.sh"]
