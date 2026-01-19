from trilogy import Executor, Dialects
from trilogy.dialect import DuckDBConfig
from trilogy_public_models.discovery import data_models
from trilogy_public_models.models import LazyEnvironment, QueryType
from pathlib import Path


def get_executor(
    model: str, executor: Executor | None = None, run_setup: bool = True
) -> Executor:
    conf = None
    if "bigquery" in model:
        dialect = Dialects.BIGQUERY
    elif "duckdb" in model:
        dialect = Dialects.DUCK_DB
        conf = DuckDBConfig(enable_gcs=True, enable_python_datasources=True)
    elif "snowflake" in model:
        dialect = Dialects.SNOWFLAKE
    else:
        raise NotImplementedError(f"Model {model} not supported")
    loaded = data_models[model]
    if executor is None:
        executor = dialect.default_executor(environment=loaded.environment, conf=conf)
    else:
        executor.environment = loaded.environment
    if isinstance(loaded.environment, LazyEnvironment):
        loaded.environment._load()

    if isinstance(loaded.setup, list):
        queries = loaded.setup
    else:
        queries = loaded.setup()
    if run_setup:
        for x in queries:
            if x.type == QueryType.SQL:
                localized_query = x.query.replace(
                    "https://trilogy-data.github.io/trilogy-public-models",
                    str(Path(__file__).parent.parent),
                )
                z = executor.execute_raw_sql(localized_query)
                z.fetchall()
            elif x.type == QueryType.TRILOGY:
                z2 = executor.execute_query(x.query)
                if z2:
                    z2.fetchall()
            else:
                z3 = executor.execute_query(x)
                if z3:
                    z3.fetchall()
    return executor
