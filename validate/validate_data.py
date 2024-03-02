import pandas as pd


def validate_data(df: pd.DataFrame, table: str) -> None:
    print("========== Start Pipeline Validation ==========")
    print("")

    # get shape of the dataframe
    n_rows = df.shape[0]
    n_cols = df.shape[1]

    print(f"Tabel {table_name} memiliki {n_rows} baris dan {n_cols} kolom")
    print("")

    cols = products_data.columns

    # get datatype for each column
    for col in cols:
        print(f"Kolom {col} memiliki tipe data {products_data[col].dtypes}")

    print("")

    # check missing values in each column
    for col in cols:
        missing_values_pct = (df[col].isnull().sum() * 100) / len(products_data)
        print(
            f"Kolom {col} memiliki missing values sebanyak {missing_values_pct}% dari total data"
        )

    print("")
    print("========== End Pipeline Validation ==========")
