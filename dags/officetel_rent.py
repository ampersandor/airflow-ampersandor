import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from contextlib import contextmanager

import requests
import psycopg2

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.hooks.postgres_hook import PostgresHook

from plugins import slack


def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    location_table = """
        CREATE TABLE IF NOT EXISTS {schema}.{officetel_location}} (
            location_id SERIAL PRIMARY KEY,
            city VARCHAR(30),
            sggnm VARCHAR(30),
            umdnm VARCHAR(30),
            UNIQUE(city, ku, dong)
        );
    """
    query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id SERIAL PRIMARY KEY,
            created_at DATE DEFAULT CURRENT_DATE,
            location_id INT REFERENCES {schema}.{officetel_location}(location_id),
            trade_ymd DATE,
            city VARCHAR(30),
            sggNm VARCHAR(30),
            umdNm VARCHAR(30),
            jibun VARCHAR(30),
            offiNm VARCHAR(30),
            floor INT,
            excluUseAr FLOAT,
            built_year INT,
            deposite INT,
            monthlyrent INT
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
def extract_officetel_rent_data(url, key, loc):
    context = get_current_context()
    date = context["logical_date"]
    logging.info(date)
    ym = date.strftime("%Y%m")
    api = f"{url}?LAWD_CD={loc}&DEAL_YMD={ym}&serviceKey={key}"
    try:
        response = requests.get(api)
        print(f"API Response Status Code: {response.status_code}")
        response.raise_for_status()
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        logging.warning(f"while requesting to {api}...")
        logging.error("Something went wrong with extract rent data!\n", e)
        raise

    return response.text


@task
def transform_officetel_rent_data(city, data):
    context = get_current_context()
    date = context["logical_date"]
    year, month, day = date.strftime("%Y %m %d").split()
    tree = ET.ElementTree(ET.fromstring(data))
    root = tree.getroot()
    rows = []
    for item in root.findall(".//item"):
        
        keys = ["dealYear", "dealMonth", "dealDay", "sggNm", "umdNm", "jibun", "offiNm", "floor", "excluUseAr", "deposit", "monthlyRent"]
        row = {k: item.find(k).text.replace(",", "").strip() if item.find(k) is not None else "NULL" for k in keys}

        trade_ymd = f"{row['dealYear']}-{row['dealMonth']}-{row['dealDay']}"
        row["trade_ymd"] = trade_ymd
        row["city"] = city
        row.pop("dealYear")
        row.pop("dealMonth")
        row.pop("dealDay")
        rows.append(row)

    return rows


@task
def load_officetel_rent_data(schema, table, rows, drop_first=False):
    
    with get_postgres_connection() as connection:
        cur = connection.cursor()
        _create_table(cur, schema, table, drop_first)
        try:
            cur.execute("BEGIN;")
            for row in rows:
                location_query = f"""
                    WITH ins AS (
                        INSERT INTO {schema}.{officetel_location} (city, sggNm, umdNm, jibun, offiNm)
                        VALUES ({", ".join(map(lambda x: f"'{x}'", [row[k] for k in ["city", "sggNm", "umdNm", "jibun", "offiNm"]]))})
                        ON CONFLICT (city, sggNm, umdNm, jibun, offiNm) DO NOTHING
                        RETURNING id
                    )
                    SELECT id FROM ins
                    UNION ALL
                    SELECT id FROM {schema}.{officetel_location}
                    WHERE city = '{row["city"]}' AND sggNm = '{row["sggNm"]}' AND umdNm = '{row["umdNm"]}' AND jibun = '{row["jibun"]}' AND offiNm = '{row["offiNm"]}'
                    LIMIT 1;
                """
                cur.execute(location_query)
                location_id = cur.fetchone()[0]
                row["location_id"] = location_id
                query = f"""
                    INSERT INTO {schema}.{table} ({", ".join(cols)})
                    VALUES ({", ".join(map(lambda x: f"'{x}'", [row[k] for k in cols]))})
                """
                logging.info(query)
                cur.execute(query)
            cur.execute("COMMIT;")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.execute("ROLLBACK;")
            raise
        logging.info("load done")


dong_code = {
    "서울특별시 송파구": "11710",
    "서울특별시 강서구": "11500",
    "서울특별시 강남구": "11680",
    "인천시 미추홀구"  : "28177",
}

translate = {"서울특별시 송파구" : "Seoul_Songpa",
            "서울특별시 강서구" : "Seoul_Gangseo",
            "서울특별시 강남구": "Seoul_Gangnam",
            "인천시 미추홀구": "Incheon_Michuhol"}

cols =  ["trade_ymd", "city","sggNm", "umdNm", "jibun", "offiNm", "floor", "excluUseAr", "deposit", "monthlyRent", "location_id"]
officetel_location = "officetel_location"
schema = "public"
table = "officetel_rent"
h = 0
for address, code in dong_code.items():
    h += 1
    with DAG(
        dag_id=f"officetel_rent_{translate[address]}",
        start_date=datetime(2021, 1, 1),
        schedule=f"0 {h} 1 * *",  # every month (day 1, 00:00)
        max_active_runs=1,
        tags=["ODIGODI", "officetel", "ETL"],
        catchup=True,
        default_args={
            "retries": 0,
            "retry_delay": timedelta(minutes=3),
            "on_failure_callback": slack.on_failure_callback,
            "on_success_callback": slack.on_success_callback,
        },
    ) as dag:

        url = Variable.get("data_portal_url_rent").strip()
        key = Variable.get("data_portal_api_key").strip()

        city, ku = address.split()
        load_officetel_rent_data(
            schema,
            table,
            transform_officetel_rent_data(
                city, extract_officetel_rent_data(url, key, code)
            ),
        )
