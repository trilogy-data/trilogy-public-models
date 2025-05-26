import os

from google.auth import default
from google.cloud import bigquery
from google.oauth2 import service_account
from trilogy.executor import Executor, Dialects
from pytest import fixture
from sqlalchemy.engine import create_engine
from typing import Generator
from trilogy.constants import Rendering
from trilogy.dialect.config import SnowflakeConfig


@fixture()
def bq_client():
    if os.path.isfile("/run/secrets/bigquery_auth"):
        credentials = service_account.Credentials.from_service_account_file(
            "/run/secrets/bigquery_auth",
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        project = credentials.project_id
    else:
        credentials, project = default()
    yield lambda: bigquery.Client(credentials=credentials, project=project)


def create_executor(bq_client):
    engine = create_engine(
        "bigquery://ttl-test-355422/test_tables?user_supplied_client=True",
        connect_args={"client": bq_client},
    )

    executor = Executor(dialect=Dialects.BIGQUERY, engine=engine)
    return executor


@fixture(scope="session")
def fakesnow_happening():
    import fakesnow

    with fakesnow.patch():
        yield


@fixture(scope="session")
def snowflake_engine(fakesnow_happening) -> Generator[Executor, None, None]:

    executor = Dialects.SNOWFLAKE.default_executor(
        conf=SnowflakeConfig(
            account="account",
            username="user",
            password="password",
            database="TAST_BYTES_SAMPLE_DATA",
            schema="RAW_POS",
        ),
        rendering=Rendering(parameters=False),
    )
    executor.execute_raw_sql(
        """CREATE DATABASE IF NOT EXISTS TASTY_BYTES_SAMPLE_DATA;
        
        
"""
    )
    executor.execute_raw_sql(
        """CREATE SCHEMA IF NOT EXISTS TASTY_BYTES_SAMPLE_DATA.RAW_POS;"""
    )
    executor.execute_raw_sql(
        """create or replace table TASTY_BYTES_SAMPLE_DATA.RAW_POS.MENU (
    MENU_ID NUMBER(19,0) NOT NULL,
    MENU_TYPE_ID NUMBER(38,0),
    MENU_TYPE VARCHAR(16777216),
    TRUCK_BRAND_NAME VARCHAR(16777216),
    MENU_ITEM_ID NUMBER(38,0),
    MENU_ITEM_NAME VARCHAR(16777216),
    ITEM_CATEGORY VARCHAR(16777216),
    ITEM_SUBCATEGORY VARCHAR(16777216),
    COST_OF_GOODS_USD NUMBER(38,4),
    SALE_PRICE_USD NUMBER(38,4),
    MENU_ITEM_HEALTH_METRICS_OBJ VARIANT
);"""
    )
    yield executor


@fixture()
def bq_executor(bq_client):
    yield lambda: create_executor(bq_client())


@fixture()
def snowflake_executor(snowflake_engine):
    yield lambda: snowflake_engine


@fixture()
def duckdb_executor():
    yield lambda: Dialects.DUCK_DB.default_executor()
