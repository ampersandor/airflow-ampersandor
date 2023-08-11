from datetime import datetime
from datetime import timedelta
import psycopg2
import logging

import yfinance as yf

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import get_current_context

from plugins import slack


def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
                upload_date DATE DEFAULT CURRENT_DATE,
                upload_time TIME DEFAULT CURRENT_TIME,
                date DATE,
                time TIME,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume INT
        );
    """
    logging.info(query)
    cur.execute(query)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id = 'myPostgres')
    return hook.get_conn().cursor()

@task
def extract(symbol):
    context = get_current_context()
    date = context["logical_date"]
    start = date.subtract(days=1).strftime("%Y-%m-%d")
    end = date.strftime("%Y-%m-%d")
    logging.info(f"start: {start}")
    logging.info(f"end: {end}")
    records = []
    try:
        data = yf.download([symbol], interval="5m", start=start, end=end)
        for index, row in data.iterrows():
            date = index.strftime('%Y-%m-%d')
            time = index.strftime('%H:%M:%S')
            records.append([date, time, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]])
    except:
        raise
    return records

@task
def load(schema, table, data, drop_first=False):
    cur = get_postgres_connection()
    try:
        cur.execute("BEGIN;")
        _create_table(cur, schema, table, drop_first)

        for row in data:
            query = f"""
                INSERT INTO stock.apple (date, time, open, high, low, close, volume)
                VALUES ('{row[0]}', '{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]})
            """
            print(query)
            cur.execute(query)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise 
    logging.info("load done")


with DAG(
    dag_id = "appleStock",
    start_date=datetime(2023, 8, 1),
    schedule="0 1 * * *", # every day (day 1, 00:00)
    max_active_runs=1,
    tags=['stock', 'apple', "ETL"],
    catchup=True,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "on_failure_callback" : slack.on_failure_callback,
        "on_success_callback" : slack.on_success_callback
    }
) as dag:
    schema = "stock"
    table = "apple"
    symbol = "AAPL"
    interval = "5min"

    data = extract(symbol)
    load(schema, table, data)