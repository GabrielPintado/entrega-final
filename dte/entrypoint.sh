#!/bin/bash

echo "Inicializando la base de datos de Airflow..."
airflow db init

echo "Iniciando el scheduler..."
airflow scheduler &

echo "Iniciando el webserver en el puerto 8081..."
exec airflow webserver --port 8081
