import luigi
import pandas as pd
from extract.extract_db import extract_db
from extract.extract_web import extract_web
from validate.validate_data import validate_data


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
