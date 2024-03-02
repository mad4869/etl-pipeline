import luigi
import pandas as pd
from extract.extract_db import extract_db
from extract.extract_web import extract_web
from validate.validate_data import validate_data
from transform.transform_products_data import transform_products_data
from transform.transform_sales_data import transform_sales_data
from transform.transform_web_text_data import transform_web_text_data


class ExtractProductsDataFromCSV(luigi.Task):
    def requires(self):
        pass

    def run(self):
        pass

    def output(self):
        return luigi.LocalTarget("data/raw/products_data.csv")


class ExtractSalesDataFromDB(luigi.Task):
    def requires(self):
        pass

    def run(self):
        df = extract_db()

        df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/raw/sales_data.csv")


class ExtractTextDataFromWeb(luigi.Task):
    def requires(self):
        pass

    def run(self):
        df = extract_web()

        df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/raw/web_text_data.csv")


class ValidateData(luigi.Task):
    def requires(self):
        return [
            ExtractProductsDataFromCSV(),
            ExtractSalesDataFromDB(),
            ExtractTextDataFromWeb(),
        ]

    def run(self):
        tables = ["PRODUCTS", "SALES", "WEB TEXT"]

        for table in tables:
            df = pd.read_csv(self.input()[i].path)

            validate_data(df, table)

    def output(self):
        pass


class TransformProductsData(luigi.Task):
    def requires(self):
        return [ExtractProductsDataFromCSV()]

    def run(self):
        df = pd.read_csv(self.input()[0].path)

        transformed_df = transform_products_data(df)

        transformed_df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/transformed/products_data.csv")


class TransformSalesData(luigi.Task):
    def requires(self):
        return [ExtractSalesDataFromDB()]

    def run(self):
        df = pd.read_csv(self.input()[0].path)

        transformed_df = transform_sales_data(df)

        transformed_df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/transformed/sales_data.csv")


class TransformWebTextData(luigi.Task):
    def requires(self):
        return [ExtractTextDataFromWeb()]

    def run(self):
        df = pd.read_csv(self.input()[0].path)

        transformed_df = transform_web_text_data(df)

        transformed_df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("data/transformed/web_text_data.csv")
