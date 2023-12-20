import argparse
import datetime as dt
import os
import sqlite3

import pandas as pd
import requests

INTERESTING_DATASETS = [
    "CO2Emis",
    "DeclarationProduction",
    "DeclarationGridmix",
    "CapacityPerMunicipality",
]
KNOWN_DATETIME_FIELDS = ["Minutes5UTC", "Minutes5DK", "HourUTC", "HourDK"]


def fetch_dataset_from_api(dataset, day, limit=10):
    start = day.strftime("%Y-%m-%d")
    end = (day + dt.timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        response = requests.get(
            f"https://api.energidataservice.dk/dataset/{dataset}?"
            + f"start={start}T00:00&end={end}&limit={limit}"
        )
        return response.json()
    except Exception:
        print("Exception during loading")
        return {}


def to_data_frame(json_dict):
    if json_dict.get("records"):
        return pd.DataFrame.from_dict(json_dict["records"])


def get_as_data_frame(dataset, day, limit=10):
    return to_data_frame(fetch_dataset_from_api(dataset, day, limit))


def field_to_datetime(df, field):
    df[field] = pd.to_datetime(df[field])


def convert_known_datetime_fields(df):
    for c in df.columns:
        if c in KNOWN_DATETIME_FIELDS:
            field_to_datetime(df, c)


def transform(df, day):
    df["date"] = day.strftime("%Y-%m-%d")
    convert_known_datetime_fields(df)
    return df.reset_index().set_index("index")


def get_and_save_as_csv(path, dataset, day, limit=10):
    file = os.path.join(path, f"{dataset}.csv")
    df = to_data_frame(fetch_dataset_from_api(dataset, day, limit))
    if df is not None:
        df.to_csv(file)


def delete_if_day_exits(conn, table, day):
    cur = conn.cursor()
    # check if table exists
    cur.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';"
    )
    if len(cur.fetchall()) > 0:
        # we already have written data for this table
        cur.execute(f"DELETE FROM {table} WHERE date = '{day.strftime('%Y-%m-%d')}';")


def upsert_into_sqlite(sqlite_db, dataset, day, df):
    with sqlite3.connect(sqlite_db) as con:
        delete_if_day_exits(con, dataset, day)
        df.to_sql(dataset, con, if_exists="append", index=False)


def etl(dataset, day, sqlite_db, limit=10000):
    df_raw = get_as_data_frame(dataset, day, limit)
    df_enriched = transform(df_raw, day)
    upsert_into_sqlite(sqlite_db, dataset, day, df_enriched)


def do_work(sqlite_db, date_start, date_end, databases):
    dates = pd.date_range(start=date_start, end=date_end)
    for db in databases:
        for day in dates:
            etl(db, day, sqlite_db)
        print(f"Database '{db}' has been loaded into {sqlite_db}.")


parser = argparse.ArgumentParser(
    prog="EnergiLoader", description="Fetches Dataset from the danish energi API"
)

parser.add_argument("sqlite_db")
parser.add_argument("date_start")
parser.add_argument("date_end")
parser.add_argument("databases")

args = parser.parse_args()
do_work(args.sqlite_db, args.date_start, args.date_end, args.databases.split(","))
