import pandas as pd


def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    # drop unnecessary columns
    columns_to_drop = ["Unnamed: 0"]

    df.drop(columns=columns_to_drop, inplace=True)

    # drop rows with missing actual price
    df.dropna(subset=["actual_price"], inplace=True)

    # fill missing values
    df.fillna(0, inplace=True)

    return df
