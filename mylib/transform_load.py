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
    access_token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("HTTP_PATH")
    # Check for missing environment variables
    if not all([server_h, access_token, http_path]):
        raise EnvironmentError(
            "Missing one or more required environment variables: SERVER_HOSTNAME, DATABRICKS_TOKEN, HTTP_PATH"
        )

    with sql.connect(
        server_hostname=server_h,
        http_path=http_path,
        access_token=access_token,
    ) as connection:
        c = connection.cursor()
        # INSERT TAKES TOO LONG
        # c.execute("DROP TABLE IF EXISTS ServeTimesDB")
        c.execute("SHOW TABLES FROM default LIKE 'majorsDB'")
        result = c.fetchall()
        # takes too long so not dropping anymore
        # c.execute("DROP TABLE IF EXISTS majorsDB")
        if not result:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS majorsDB (
                    FOD1P int,
                    Major string,
                    Major_Category string
                )
            """
            )
            # insert
            for _, row in df.iterrows():
                convert = (_,) + tuple(row)
                c.execute(f"INSERT INTO majorsDB VALUES {convert}")
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
            for _, row in df2.iterrows():
                convert = (_,) + tuple(row)
                c.execute(f"INSERT INTO womenstemDB VALUES {convert}")
        c.close()

    return "success"


if __name__ == "__main__":
    load()
