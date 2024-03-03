import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def load_data(df: pd.DataFrame, table: str) -> pd.DataFrame:
    LOCAL_DB_NAME = os.getenv("LOCAL_DB_NAME")
    LOCAL_DB_USER = os.getenv("LOCAL_DB_USER")
    LOCAL_DB_PASSWORD = os.getenv("LOCAL_DB_PASSWORD")
    LOCAL_DB_HOST = os.getenv("LOCAL_DB_HOST")
    LOCAL_DB_PORT = os.getenv("LOCAL_DB_PORT")

    conn = create_engine(
        f"postgresql://{LOCAL_DB_USER}:{LOCAL_DB_PASSWORD}@{LOCAL_DB_HOST}:{LOCAL_DB_PORT}/{LOCAL_DB_NAME}"
    )

    df.to_sql(name=table, con=conn, if_exists="replace", index=False)

    conn.dispose()

    return df
