import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


def load_data(df: pd.DataFrame, table: str) -> pd.DataFrame:
    LOCAL_DB_NAME = os.getenv("LOCAL_DB_NAME")
    LOCAL_DB_USER = os.getenv("LOCAL_DB_USER")
    LOCAL_DB_PASSWORD = os.getenv("LOCAL_DB_PASSWORD")
    LOCAL_DB_HOST = os.getenv("LOCAL_DB_HOST")
    LOCAL_DB_PORT = os.getenv("LOCAL_DB_PORT")

    conn = psycopg2.connect(
        dbname=LOCAL_DB_NAME,
        user=LOCAL_DB_USER,
        password=LOCAL_DB_PASSWORD,
        host=LOCAL_DB_HOST,
        port=LOCAL_DB_PORT,
    )

    df.to_sql(name=table, con=conn, if_exists="append", index=False)

    conn.close()

    return df
