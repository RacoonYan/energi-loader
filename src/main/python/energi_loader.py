import argparse
import datetime as dt
import os
import sqlite3

import pandas as pd
import requests

# Extracting the data of different datasets from the API of TSO electricity from denmark
KNOWN_DATETIME_FIELDS = ["Minutes5UTC", "Minutes5DK", "HourUTC", "HourDK"]


def fetch_dataset_from_api(dataset, day, limit=10):
    start = day.strftime("%Y-%m-%d")
    end = (day + dt.timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        response = requests.get(
            f"https://api.energidataservice.dk/dataset/{dataset}?"
            + f"start={start}T00:00&end={end}&limit={limit}"
        )
        json_dict = response.json()
        if json_dict.get("total", 0) > limit:
            print(
                f"""Warning not all records were fetched for
                {dataset} on {day} increase the limit."""
            )
        return json_dict
    except IOError as err:
        print(f"Exception during loading, {err}")
        return {}


# transform the json data into dataframe without the matadata
def to_data_frame(json_dict):
    """strip metadata from response
    {
        "total": 1465496,
        "limit": 4,
        "dataset": "CO2Emis",
        "records": [
            {
            "Minutes5UTC": "2023-12-20T12:00:00",
            "Minutes5DK": "2023-12-20T13:00:00",
            "PriceArea": "DK1",
            "CO2Emission": 94.000000
            },
            ...
            }
        ]
    }"""
    if json_dict.get("records"):
        return pd.DataFrame.from_dict(json_dict["records"])


def get_as_data_frame(dataset, day, limit=10):
    return to_data_frame(fetch_dataset_from_api(dataset, day, limit))


# convert string to dataframe for known datetime fields
def convert_known_datetime_fields(df):
    for c in df.columns:
        if c in KNOWN_DATETIME_FIELDS:
            df[c] = pd.to_datetime(df[c])


def transform(df, day):
    df["date"] = day.strftime("%Y-%m-%d")
    convert_known_datetime_fields(df)
    return df.reset_index().set_index("index")


def get_and_save_as_csv(path, dataset, day, limit=10):
    file = os.path.join(path, f"{dataset}.csv")
    df = to_data_frame(fetch_dataset_from_api(dataset, day, limit))
    if df is not None:
        df.to_csv(file)

# avoiding duplicating tables by downloading the data everytime 
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
    with sqlite3.connect(sqlite_db) as conn:
        delete_if_day_exits(conn, dataset, day)
        df.to_sql(dataset, conn, if_exists="append", index=False)

# define the etl 
def etl(dataset, day, sqlite_db, limit=10000):
    df_raw = get_as_data_frame(dataset, day, limit)
    if df_raw is None:
        print(f"No data for {dataset} on {day}")
        return
    df_enriched = transform(df_raw, day)
    upsert_into_sqlite(sqlite_db, dataset, day, df_enriched)

# based on the needs of dateframe as well as datasets to get the desire database
def do_work(sqlite_db, date_start, date_end, datasets):
    dates = pd.date_range(start=date_start, end=date_end)
    for db in datasets:
        # Progress dots are printed to indicate the loading progress
        print(f"Loading data for {db} ...", end="", flush=True) 
        for day in dates:
            etl(db, day, sqlite_db)
            print(".", end="", flush=True)
        print("")
        print(f"Database '{db}' has been loaded into {sqlite_db}.")

if __name__ == "__main__":
    #create a command-line argument parser
    parser = argparse.ArgumentParser(
        prog="EnergiLoader", description="Fetches datasets from the danish energi API"
    )
    # define the command-line arguments 
    parser.add_argument("sqlite_db")
    parser.add_argument("date_start")
    parser.add_argument("date_end")
    parser.add_argument("datasets")
    # parse the command-line arguments provided when the script is executed.
    args = parser.parse_args()
    do_work(args.sqlite_db, args.date_start, args.date_end, args.datasets.split(","))
