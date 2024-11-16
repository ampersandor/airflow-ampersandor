""" Full Refresh """

import logging
from datetime import datetime, timedelta
from contextlib import contextmanager

import requests
import psycopg2
import json

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

from plugins import slack


def _create_table(cur, schema, table, drop_first=False):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")

    query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            upload_date DATE DEFAULT CURRENT_DATE,
            upload_time TIME DEFAULT CURRENT_TIME,
            location_id SERIAL PRIMARY KEY,
            city VARCHAR(30),
            ku VARCHAR(30),
            dong VARCHAR(30),
            jicode VARCHAR(30),
            name VARCHAR(30),
            lat FLOAT,
            lng FLOAT,
            UNIQUE (city, ku, dong, jicode, name)
        );
    """
    logging.info(query)
    cur.execute(query)


@contextmanager
def get_postgres_connection():
    connection = PostgresHook(postgres_conn_id="odigodi-supabase").get_conn()
    try:
        yield connection
    finally:
        connection.close()


@task
def extract_address(schema, table):
    with get_postgres_connection() as connection:
        cur = connection.cursor()
        try:
            query = f"""
                SELECT id as location_id, city, sggNm, umdNm, jibun, offiNm FROM {schema}.{table};
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
    for location_id, city, ku, dong, jicode, name in rows:
        address = f"{city} {ku} {dong} {jicode}"
        api = f"{url}?query=" + address
        headers = {
            "Authorization": f"KakaoAK {key}",
        }
        try:
            response = requests.get(api, headers=headers)
            response.raise_for_status()
            api_json = json.loads(str(response.text))
            logging.info(api_json)
            if not api_json["documents"]:
                logging.warning(
                    f"the {api} has no data for {address}, skipping it. \n {api_json}"
                )
                continue
        except requests.exceptions.RequestException as e:
            print(e)
            raise
        else:
            logging.info(api_json)
            address = api_json["documents"][0]["address"]
            lng, lat = address["x"], address["y"]
            data.append((location_id, lat, lng))

    return data


@task
def load_lat_long(schema, table, data):
    with get_postgres_connection() as connection:
        cur = connection.cursor()
        _create_table(cur, schema, table)

        try:
            cur.execute("BEGIN;")
            for location_id, lat, lng in data:
                location_query = f"""
                    UPDATE {schema}.{table}
                    SET lat = %s, lng = %s
                    WHERE id = %s
                    RETURNING id;
                """
                values = (lat, lng, location_id)

                cur.execute(location_query, values)
            cur.execute("COMMIT;")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.execute("ROLLBACK;")
            raise
        logging.info("load done")


schema = "public"
table = "officetel_location"
with DAG(
    dag_id="officetel_location",
    start_date=datetime(2021, 1, 1),
    schedule="0 0 2 * *",  # every month (day 2, 00:00)
    max_active_runs=1,
    tags=["ODIGODI", "officetel", "ETL"],
    catchup=False,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "on_failure_callback": slack.on_failure_callback,
        "on_success_callback": slack.on_success_callback,
    },
) as dag:

    url = Variable.get("kakao_map_url")
    key = Variable.get("kakao_map_api_key")

    rows = extract_address(schema, table)
    data = extract_lat_long(rows, url, key)

    load_lat_long(schema, table, data)
