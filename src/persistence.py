import logging
import os
import pandas as pd
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger("PERSISTENCE")
logger.setLevel(logging.INFO)


SF_CREDENTIALS = dict(
    account=os.getenv("SF_ACCOUNT"),
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    role=os.getenv("SF_ROLE"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    database=os.getenv("SF_DATABASE"),
    schema=os.getenv("SF_SCHEMA")
)

schema = os.getenv("SF_SCHEMA")
conn = sf.connect(**SF_CREDENTIALS)


def upload_df(df: pd.DataFrame, table_name: str, schema_name: str, database: str = "NYT") -> None:
    full_table_name = f"{database}.{schema_name}.{table_name}"

    logger.info(f"Loading data into {full_table_name}")
    success, nchunks, nrows, _ = write_pandas(
            conn,
            df.reset_index(drop=True),
            schema=schema_name,
            table_name=table_name,
            auto_create_table=True,
            overwrite=True,
        )

    if success:
        logger.info(f"Successfully loaded {nrows} rows into {full_table_name} in Snowflake")
    else:
        logger.warning(f"Failed to upload {full_table_name} into Snowflake")

