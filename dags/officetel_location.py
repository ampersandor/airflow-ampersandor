""" Full Refresh """
import logging
from datetime import datetime
from datetime import timedelta

import requests
import psycopg2
import json

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

from plugins import slack



def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            upload_date DATE DEFAULT CURRENT_DATE,
            upload_time TIME DEFAULT CURRENT_TIME,
            ku VARCHAR(30),
            dong VARCHAR(30),
            jicode VARCHAR(30),
            name VARCHAR(30),
            lat FLOAT,
            lng FLOAT
        );
    """
    logging.info(query)
    cur.execute(query)


def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id = 'odigodi_postgres')
    return hook.get_conn().cursor()


@task
def extract_address(schema, table1, table2):
    cur = get_postgres_connection()

    try:
        query = f"""
            SELECT DISTINCT city, ku, dong, jicode, name FROM {schema}.{table1} where monthly_pay=0
            UNION SELECT DISTINCT city, ku, dong, jicode, name FROM {schema}.{table2};
        """
        cur.execute(query)
        result = cur.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise     
    logging.info("load done")
    return result


@task
def extract_lat_long(rows, url, key):
    data = []
    for city, ku, dong, jicode, name in rows:
        address = f"{city} {ku} {dong} {jicode}"
        api = f"{url}?query=" + address
        headers = {
            "Authorization": f'KakaoAK {key}',
        }
        try:
            response = requests.get(api, headers=headers)
            response.raise_for_status()
            api_json = json.loads(str(response.text))
            logging.info(api_json)
            if not api_json["documents"]:
                logging.warning(f"the {api} has no data for {address}, skipping it. \n {api_json}")
                continue
        except requests.exceptions.RequestException as e:
            print(e)
            raise
        else:
            logging.info(api_json)
            address = api_json["documents"][0]["address"]
            lat, long = address['x'], address['y']
            data.append((ku, dong, jicode, name, lat, long))

    return data

@task
def load_lat_long(schema, table, data, drop_first=True):
    cur = get_postgres_connection()
    _create_table(cur, schema, table, drop_first)

    try:
        cur.execute("BEGIN;")
        for row in data:
            query = f"""
                INSERT INTO {schema}.{table} (ku, dong, jicode, name, lat, lng)
                VALUES ('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', {row[4]}, {row[5]})
            """
            logging.info(query)
            cur.execute(query)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise 
    logging.info("load done")


with DAG(
    dag_id = "officetel_location",
    start_date=datetime(2021, 1, 1),
    schedule="0 0 2 * *", # every month (day 2, 00:00)
    max_active_runs=1,
    tags=['ODIGODI', 'officetel', "ETL"],
    catchup=False,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "on_failure_callback" : slack.on_failure_callback,
        "on_success_callback": slack.on_success_callback
    }
) as dag:
    schema = "officetel"
    table1 = "rent"
    table2 = "trade"

    url = Variable.get("kakao_map_url")
    key = Variable.get("kakao_map_api_key")
    
    rows = extract_address(schema, table1, table2)
    data = extract_lat_long(rows, url, key)

    table = "location"
    load_lat_long(schema, table, data)


