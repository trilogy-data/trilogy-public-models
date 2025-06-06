from trilogy import Executor, Dialects
from trilogy_public_models.discovery import data_models
from trilogy_public_models.models import LazyEnvironment, QueryType


def get_executor(
    model: str, executor: Executor | None = None, run_setup: bool = True
) -> Executor:

    if "bigquery" in model:
        dialect = Dialects.BIGQUERY
    elif "duckdb" in model:
        dialect = Dialects.DUCK_DB
    elif "snowflake" in model:
        dialect = Dialects.SNOWFLAKE
    else:
        raise NotImplementedError(f"Model {model} not supported")
    loaded = data_models[model]
    if executor is None:
        executor = dialect.default_executor(environment=loaded.environment)
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
                z = executor.execute_raw_sql(x.query)
                z.fetchall()
            elif x.type == QueryType.TRILOGY:
                z = executor.execute_query(x.query)
                z.fetchall()
            else:
                z = executor.execute_query(x)
                z.fetchall()
    return executor
