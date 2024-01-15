"""
SELECT name, area, AVG(deposite) AS average_deposit FROM (   SELECT name, area, deposite, ROW_NUMBER() OVER (PARTITION BY name, area ORDER
BY trade_ymd DESC) AS rn   FROM rent   WHERE dong = '방이동' AND monthly_pay = 0 ) subquery WHERE rn <= 30 GROUP BY name, area;
"""

import logging
from datetime import datetime
from contextlib import contextmanager

import psycopg2

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

from plugins import slack


@contextmanager
def get_postgres_connection():
    connection = PostgresHook(postgres_conn_id="odigodi_postgres").get_conn()
    try:
        yield connection
    finally:
        connection.close()


@task
def load(schema, table, query):
    with get_postgres_connection() as connection:
        cur = connection.cursor()
        try:
            cur.execute("BEGIN;")
            query = f"""DROP TABLE IF EXISTS {schema}.{table}_temp;
                        CREATE TABLE {schema}.{table}_temp AS {query}"""
            logging.info(query)
            cur.execute(query)

            query = f"""SELECT COUNT(1) FROM {schema}.{table}_temp"""
            logging.info(query)
            cur.execute(query)
            count = cur.fetchone()[0]
            if count == 0:
                raise ValueError(f"{table} didn't have any record")

            query = f"""DROP TABLE IF EXISTS {schema}.{table};
                    ALTER TABLE {schema}.{table}_temp RENAME to {table};"""
            logging.info(query)
            cur.execute(query)
            cur.execute("COMMIT;")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.execute("ROLLBACK;")
            raise
        logging.info("load done")


with DAG(
    dag_id="officetel_elt",
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    tags=["ODIGODI", "prod", "ELT"],
    default_args={
        "retries": 0,
        "on_failure_callback": slack.on_failure_callback,
        "on_success_callback": slack.on_success_callback,
    },
) as dag:
    warehouse_schema = "officetel"
    ui_schema = "public"

    table = "trade"
    query = f"""select trade_ymd, name, area, price from {warehouse_schema}.{table} order by name, area, trade_ymd;"""
    load(ui_schema, table, query)

    table = "rent"
    query = f"""select trade_ymd, name, area, deposite from {warehouse_schema}.{table} where monthly_pay=0 order by name, area, trade_ymd;"""
    load(ui_schema, table, query)

    table = "location"
    query = f"""select name, lat, lng from {warehouse_schema}.{table} where lat is not NULL and lng is not NULL;"""
    load(ui_schema, table, query)
