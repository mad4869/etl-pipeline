import os
import luigi
import pandas as pd
from extract.extract_db import extract_db
from extract.extract_web import extract_web
from validate.validate_data import validate_data
from transform.transform_products_data import transform_products_data
from transform.transform_sales_data import transform_sales_data
from transform.transform_web_text_data import transform_web_text_data
from load.load_data import load_data

class ForceableTask(luigi.Task):
    force = luigi.BoolParameter(significant=False, default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.force is True:
            outputs = luigi.task.flatten(self.output())
            for out in outputs:
                if out.exists():
                    os.remove(out.path)


class ExtractProductsDataFromCSV(luigi.Task):
    def requires(self):
        pass

    def run(self):
        pass

    def output(self):
        return luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/raw/products_data.csv")


class ExtractSalesDataFromDB(ForceableTask):
    def requires(self):
        pass

    def run(self):
        df = extract_db()

        df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/raw/sales_data.csv")


class ExtractTextDataFromWeb(luigi.Task):
    def requires(self):
        pass

    def run(self):
        df = extract_web()

        df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/raw/web_text_data.csv")


class ValidateData(luigi.Task):
    def requires(self):
        return [
            ExtractProductsDataFromCSV(),
            ExtractSalesDataFromDB(),
            ExtractTextDataFromWeb(),
        ]

    def run(self):
        tables = ["PRODUCTS", "SALES", "WEB TEXT"]

        for index, table in enumerate(tables):
            df = pd.read_csv(self.input()[index].path)

            validate_data(df, table)

    def output(self):
        return [
            luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/validated/products_data.csv"),
            luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/validated/sales_data.csv"),
            luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/validated/web_text_data.csv"),
        ]

class TransformProductsData(luigi.Task):
    def requires(self):
        return ExtractProductsDataFromCSV()

    def run(self):
        df = pd.read_csv(self.input().path)

        transformed_df = transform_products_data(df)

        transformed_df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/transformed/products_data.csv")


class TransformSalesData(ForceableTask):
    def requires(self):
        return ExtractSalesDataFromDB()

    def run(self):
        df = pd.read_csv(self.input().path)

        transformed_df = transform_sales_data(df)

        transformed_df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/transformed/sales_data.csv")


class TransformWebTextData(luigi.Task):
    def requires(self):
        return ExtractTextDataFromWeb()

    def run(self):
        df = pd.read_csv(self.input().path)

        transformed_df = transform_web_text_data(df)

        transformed_df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/transformed/web_text_data.csv")


class LoadData(ForceableTask):

    def requires(self):
        return [TransformProductsData(), TransformSalesData(), TransformWebTextData()]

    def run(self):
        products_df = pd.read_csv(self.input()[0].path)
        sales_df = pd.read_csv(self.input()[1].path)
        web_text_df = pd.read_csv(self.input()[2].path)

        loaded_products_df = load_data(products_df, "products")
        loaded_sales_df = load_data(sales_df, "sales")
        loaded_web_text_df = load_data(web_text_df, "web_text")

        loaded_products_df.to_csv(self.output()[0].path, index=False)
        loaded_sales_df.to_csv(self.output()[1].path, index=False)
        loaded_web_text_df.to_csv(self.output()[2].path, index=False)

    def output(self):
        return [
            luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/loaded/products_data.csv"),
            luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/loaded/sales_data.csv"),
            luigi.LocalTarget("/home/mad4869/Documents/pacmann/data-engineering/etl-pipeline/data/loaded/web_text_data.csv"),
        ]


if __name__ == "__main__":
    luigi.build(
        [
            ExtractProductsDataFromCSV(),
            ExtractSalesDataFromDB(force=True),
            ExtractTextDataFromWeb(),
            ValidateData(),
            TransformProductsData(),
            TransformSalesData(force=True),
            TransformWebTextData(),
            LoadData(force=True),
        ]
    )
