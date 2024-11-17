import os

from google.auth import default
from google.cloud import bigquery
from google.oauth2 import service_account
from trilogy.executor import Executor, Dialects
from pytest import fixture
from sqlalchemy.engine import create_engine


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


@fixture()
def bq_executor(bq_client):
    yield lambda: create_executor(bq_client())


@fixture()
def duckdb_executor():
    yield lambda: Dialects.DUCK_DB.default_executor()
