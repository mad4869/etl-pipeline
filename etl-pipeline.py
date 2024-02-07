import luigi
from extract.extract_db import extract_db
from extract.extract_web import extract_web


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
