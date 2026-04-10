import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL

sheet_id = os.getenv("GOOGLE_SHEET_ID")
gid = os.getenv("GOOGLE_SHEET_GID")
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")


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
    return df


def get_engine():
    snowflake_url = URL(
        account=require_env("SNOWFLAKE_ACCOUNT"),
        user=require_env("SNOWFLAKE_USER"),
        password=require_env("SNOWFLAKE_PASSWORD"),
        database=require_env("SNOWFLAKE_DATABASE"),
        schema=require_env("SNOWFLAKE_SCHEMA"),
        warehouse=require_env("SNOWFLAKE_WAREHOUSE"),
        role=require_env("SNOWFLAKE_ROLE"),
    )
    return create_engine(snowflake_url)


def upload_to_snowflake(df: pd.DataFrame) -> None:
    table_name = os.getenv("SNOWFLAKE_TABLE", "HARVEST")

    engine = get_engine()
    connection = None
    try:
        connection = engine.connect()
        connection.execute(text("SELECT CURRENT_VERSION()"))
        print("Successfully connected to Snowflake.")

        # Write DataFrame to Snowflake
        df.to_sql(table_name, con=engine, index=False, if_exists="replace", method="multi")
        print(
            f"DataFrame successfully uploaded to "
            f"{require_env('SNOWFLAKE_DATABASE')}.{require_env('SNOWFLAKE_SCHEMA')}.{table_name}"
        )
    finally:
        if connection is not None:
            connection.close()
        engine.dispose()
        print("Snowflake connection closed.")


if __name__ == "__main__":
    try:
        data = load_google_sheet()
        upload_to_snowflake(data)
    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        sys.exit(1)
