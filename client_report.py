import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import pyarrow as pa


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value


def load_google_sheet() -> pd.DataFrame:
    spreadsheet_id = require_env("GOOGLE_SHEET_ID")
    gid = require_env("GOOGLE_SHEET_GID")

    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
    print(f"Reading data from Google Sheet gid={gid}...")

    df = pd.read_csv(url)
    print(f"Loaded {len(df)} rows and {len(df.columns)} columns.")
    print(df.head())

    # Clean column names for Snowflake
    df.columns = [
        col.strip()
           .replace(" ", "_")
           .replace("/", "_")
           .replace("-", "_")
           .replace(".", "_")
           .upper()
        for col in df.columns
    ]

    return df


def get_connection():
    return snowflake.connector.connect(
        account=require_env("SNOWFLAKE_ACCOUNT"),
        user=require_env("SNOWFLAKE_USER"),
        password=require_env("SNOWFLAKE_PASSWORD"),
        warehouse=require_env("SNOWFLAKE_WAREHOUSE"),
        database=require_env("SNOWFLAKE_DATABASE"),
        schema=require_env("SNOWFLAKE_SCHEMA"),
        role=require_env("SNOWFLAKE_ROLE"),
    )


def upload_to_snowflake(df: pd.DataFrame) -> None:
    table_name = require_env("SNOWFLAKE_TABLE").upper()
    database = require_env("SNOWFLAKE_DATABASE")
    schema = require_env("SNOWFLAKE_SCHEMA")

    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        result = cur.fetchone()
        print("Connected to Snowflake:")
        print(f"Database: {result[0]}")
        print(f"Schema: {result[1]}")
        print(f"Warehouse: {result[2]}")
        print(f"Role: {result[3]}")

        # Drop table if replacing
        cur.execute(f"DROP TABLE IF EXISTS {database}.{schema}.{table_name}")
        print(f"Dropped existing table if it existed: {database}.{schema}.{table_name}")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=False
        )

        if not success:
            raise RuntimeError("write_pandas reported failure.")

        print(f"Uploaded {nrows} rows in {nchunks} chunk(s).")
        print(f"DataFrame successfully uploaded to {database}.{schema}.{table_name}")

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        print("Snowflake connection closed.")


if __name__ == "__main__":
    try:
        data = load_google_sheet()
        upload_to_snowflake(data)
    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        sys.exit(1)
