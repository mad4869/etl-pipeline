import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


def extract_db() -> pd.DataFrame:
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

    query = "SELECT * FROM amazon_sales_data"

    df = pd.read_sql(query, conn)

    conn.close()

    return df
