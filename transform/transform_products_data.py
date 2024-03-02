import pandas as pd


def transform_products_data(df: pd.DataFrame) -> pd.DataFrame:
    # drop unnecessary columns
    columns_to_drop = [
        "ean",
        "Unnamed: 26",
        "Unnamed: 27",
        "Unnamed: 28",
        "Unnamed: 29",
        "Unnamed: 30",
    ]

    df.drop(columns=columns_to_drop, inplace=True)

    # fill missing values
    df.fillna("Not Found", inplace=True)

    return df
