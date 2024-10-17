"""
Transforms and Loads data into the azure databricks
"""

import os
from dotenv import load_dotenv
from databricks import sql
import pandas as pd


# load the csv file and insert into databricks
def load(dataset="data/majors.csv", dataset2="data/womenstem.csv"):
    """Transforms and Loads data into the local databricks database"""
    df = pd.read_csv(dataset, delimiter=",", skiprows=1)
    df2 = pd.read_csv(dataset2, delimiter=",", skiprows=0)
    df2 = df2[["Rank", "Major", "Men", "Women", "Total", "Median"]]
    df2 = df2.iloc[1:]
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("DATABRICKS_KEY")
    http_path = os.getenv("HTTP_PATH")

    with sql.connect(
        server_hostname=server_h,
        http_path=http_path,
        access_token=access_token,
    ) as connection:
        c = connection.cursor()
        # INSERT TAKES TOO LONG
        c.execute("DROP TABLE IF EXISTS majorsDB")
        c.execute("SHOW TABLES FROM default LIKE 'majorsDB*'")
        result = c.fetchall()
        # takes too long so not dropping anymore
        # c.execute("DROP TABLE IF EXISTS majorsDB")
        if not result:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS majorsDB (
                    FOD1P string,
                    Major string,
                    Major_Category string
                )
            """
            )
            # insert
            majors_data = [tuple(row) for _, row in df.iterrows()]
            insert_query = (
                "INSERT INTO majorsDB (FOD1P, Major, Major_Category) VALUES (?, ?, ?)"
            )
            c.executemany(insert_query, majors_data)
        c.execute("DROP TABLE IF EXISTS womenstemDB")
        c.execute("SHOW TABLES FROM default LIKE 'womenstemDB*'")
        result = c.fetchall()
        # c.execute("DROP TABLE IF EXISTS womenstemDB")
        if not result:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS womenstemDB (
                    Rank int,
                    Major string,
                    Total int,
                    Men string,
                    Women string,
                    ShareWomen float,
                    Median int
                )
                """
            )
            womenstem_data = [tuple(row) for _, row in df2.iterrows()]
            insert_query2 = "INSERT INTO womenstemDB (Rank, Major, Men, Women, Total, Median) VALUES (?, ?, ?, ?, ?, ?)"
            c.executemany(insert_query2, womenstem_data)
        c.close()
    print("success")
    return "success"


if __name__ == "__main__":
    load()
