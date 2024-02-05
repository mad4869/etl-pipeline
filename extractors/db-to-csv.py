import asyncio
from prisma import Prisma
import pandas as pd


async def main() -> None:
    db = Prisma(auto_register=True)
    await db.connect()

    sales_data = await db.amazon_sales_data.find_many()
    sales_data_dicts = [dict(item) for item in sales_data]

    sales_data_df = pd.DataFrame(sales_data_dicts)
    sales_data_df.to_csv("data/sales_data.csv", index=False)

    await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
