# Proyecto de Airflow para Extracción y Carga de Datos por medio de una Api pública y amazon redshift

Este proyecto contiene un DAG de Apache Airflow diseñado para extraer datos de una base de datos Redshift y una api pública
para posteriormente guardarlos en un archivo CSV. El proyecto se ejecuta en un contenedor Docker.

## Estructura del Proyecto

- `dags/`: Contiene los archivos del DAG.
  - `prin.py`: DAG de Airflow para la extracción de datos.
  - `__init__.py`: Inicializador para el directorio `dags`.
- `Dockerfile`: Configura el entorno de Docker con Apache Airflow.
- `entrypoint.sh`: Script de entrada para inicializar Airflow y ejecutar el scheduler y el webserver.
- `requirements.txt`: Lista de dependencias de Python.

## Instalación y Configuración

### Prerequisitos

- Docker y Docker Compose deben estar instalados en tu equipo.

### Construcción de la Imagen Docker

por medio de la consola:

1. Construye la imagen Docker:

   ```bash
   docker build -t dten .

2.Inicia el servidor 

	docker run -d -p 8081:8081 dten

3.Crea un usuario

docker exec -it <container_id> airflow users create \
    --username <USERNAME> \
    --firstname <FIRSTNAME> \
    --lastname <LASTNAME> \
    --email <EMAIL> \
    --role Admin \
    --password <PASSWORD>

4. Inicia sesion en el servidor de airflow con la cuenta creada

#P.D: en caso de algún fallo con el DAG, ir a los archivos de el contenedor de Docker y en el archivo 
root//airflow/airflow.cfg cerciorarse de que la dirección en dags_folder sea igual a /usr/local/airflow/dags

5.Una vez la lista de dags haya cargado, buscar weather_data_dag e iniciarlo

#P.D: en caso de haber presentado el error mencionado anteriormente, luego de modificar los archivos
dirigirse a la seccion dags de la pagina y refrescar.

6.Una vez iniciado weather_data_dag buscar en los archivos de docker:

local/airflow/dags/combined_data_weather_data.CSV

este archivo contiene los datos extraidos.
