import json
import os
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.


default_args = {
    'owner': 'facundodipaolo',
    'depends_on_past': True,
    'start_date': datetime(2023, 10, 2),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


API_KEY = os.environ.get('API_KEY')
WAREHOUSE_HOST = os.environ.get('WAREHOUSE_HOST')
WAREHOUSE_DBNAME = os.environ.get('WAREHOUSE_DBNAME')
WAREHOUSE_USER = os.environ.get('WAREHOUSE_USER')
WAREHOUSE_PASSWORD = os.environ.get('WAREHOUSE_PASSWORD')
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
EMAIL_FROM = os.environ.get('EMAIL_FROM')
EMAIL_TO = os.environ.get('EMAIL_TO')


def save_data_to_dw(**context):

    ti = context['ti']
    df = pd.DataFrame(ti.xcom_pull(task_ids='get_data', key='get_data'))

    conn_string = f"host='{WAREHOUSE_HOST}' dbname='{WAREHOUSE_DBNAME}' user='{WAREHOUSE_USER}' password='{WAREHOUSE_PASSWORD}' port=5439"
    conn = psycopg2.connect(conn_string)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS weather (
                        city_id integer NOT NULL,
                        city_name varchar(25) NOT NULL,
                        country char(2) NOT NULL,
                        temperature decimal(3,1) NOT NULL,
                        temperature_min decimal(3,1) NOT NULL,
                        temperature_max decimal(3,1) NOT NULL,
                        humidity integer NOT NULL,
                        pressure integer NOT NULL,
                        wind_speed decimal(4,2) NOT NULL,
                        cloudiness integer NOT NULL,
                        rain_last_3h decimal (4,2),
                        query_timestamp date NOT NULL,
	                    PRIMARY KEY (city_id, query_timestamp)
                    )"""
            )

            city_id = str(df.loc[1, "city_id"])
            query_timestamp = str(df.loc[1, "query_timestamp"])

            # CHEQUEO SI YA EXISTE LA COMPOSITE KEY
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM weather WHERE city_id = %s AND query_timestamp = %s)",
                (city_id, query_timestamp)
            )
            exists = cur.fetchone()[0]

            if not exists:
                # INSERTA EL BATCH SI NO EXISTE:
                engine = create_engine(
                    f'postgresql://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}@{WAREHOUSE_HOST}:5439/{WAREHOUSE_DBNAME}')
                df.to_sql('weather', engine, if_exists="append", index=False)
            else:
                print(
                    f"Los datos para hoy {query_timestamp} ya fueron cargados")


def extract_data_from_api(**context):
    abs_path = os.path.dirname(os.path.abspath(__file__))

    with open(f'{abs_path}/localidades.json', 'r', encoding="utf-8") as json_localidades:

        localidades = json.load(json_localidades)

    log_info_localidades = []

    for localidad in localidades:

        city_name = localidad['city_name']

        lat = localidad["lat"]

        lon = localidad["lon"]

        response_API = requests.get(
            f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric', timeout=25)

        if response_API.status_code == 200:

            data = json.loads(response_API.text)

            rain_3h = data.get("rain", {}).get("3h", None)

            # dt = datetime.fromtimestamp(data["dt"])
            dt = datetime.fromtimestamp(data["dt"]).strftime("%Y-%m-%d")

            log_info_localidades.append(
                {
                    'city_id': data["id"],
                    'city_name': data["name"],
                    'country': data["sys"]["country"],
                    'temperature': data["main"]['temp'],
                    'temperature_min': data["main"]['temp_min'],
                    'temperature_max': data["main"]['temp_max'],
                    'humidity': data["main"]['humidity'],
                    'pressure': data["main"]['pressure'],
                    'wind_speed': data["wind"]['speed'],
                    'cloudiness': data["clouds"]['all'],
                    'rain_last_3h': rain_3h,
                    'query_timestamp': dt
                }
            )

            context['ti'].xcom_push(key='get_data', value=log_info_localidades)

        else:
            print(f'API falló para {city_name}: {response_API.text}')
            return None


def verify_threshold(**context):

    ti = context['ti']
    weather_df = pd.DataFrame(ti.xcom_pull(
        task_ids='get_data', key='get_data'))

    # Cargar los límites de temperatura desde el archivo JSON
    abs_path = os.path.dirname(os.path.abspath(__file__))
    with open(f'{abs_path}/limites_temp.json', 'r', encoding="utf-8") as file:
        temperature_limits = json.load(file)

    for index, row in weather_df.iterrows():
        city_name = row['city_name']
        actual_temp = row['temperature']

        city_name = city_name.lower().replace(" ", "_")

        if city_name in temperature_limits:
            min_temp = temperature_limits[city_name].get('min_temp', None)
            max_temp = temperature_limits[city_name].get('max_temp', None)
            city_name = city_name.replace('_', ' ')

            if min_temp is not None and max_temp is not None:
                if actual_temp < min_temp:
                    message = Mail(
                        from_email=EMAIL_FROM,
                        to_emails=EMAIL_TO,
                        subject=f"Alerta por baja temperatura en la ciudad de {city_name}",
                        html_content=f"Alerta por temperatura extrema: {actual_temp}°C en la ciudad de {city_name}. "
                        f"Los límites de alerta de temperatura para {city_name} están configurados en "
                        f"min: {min_temp}°C y max: {max_temp}°C.")

                    sg = SendGridAPIClient(SENDGRID_API_KEY)
                    response = sg.send(message)
                    print(response.status_code)
                    print("MENSAJE SE HA ENVIADO")

                elif actual_temp > max_temp:
                    message = Mail(
                        from_email=EMAIL_FROM,
                        to_emails=EMAIL_TO,
                        subject=f"Alerta por alta temperatura en la ciudad de {city_name}",
                        html_content=f"Alerta por temperatura extrema: {actual_temp}°C en la ciudad de {city_name}. "
                        f"Los límites de alerta de temperatura para {city_name} están configurados en "
                        f"min: {min_temp}°C y max: {max_temp}°C.")

                    sg = SendGridAPIClient(SENDGRID_API_KEY)
                    response = sg.send(message)
                    print(response.status_code)

                else:
                    # No hay alerta para esta ciudad
                    print("no hay alerta")
                    continue
            else:
                # Los límites de temperatura no están definidos para esta ciudad
                continue
        else:
            # La ciudad no se encuentra en los límites definidos
            continue


with DAG(dag_id='dag_proyecto_DE',
         description='DAG que extrae datos de una api de clima y los almacena en una db',
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args) as dag:

    get_data = PythonOperator(
        task_id='get_data', python_callable=extract_data_from_api, dag=dag, provide_context=True)

    save_data = PythonOperator(
        task_id='save_data', python_callable=save_data_to_dw, dag=dag, provide_context=True)

    send_email_if_anomaly = PythonOperator(
        task_id='send_email_if_anomaly', python_callable=verify_threshold, dag=dag, provide_context=True)

    get_data >> save_data
    get_data >> send_email_if_anomaly
