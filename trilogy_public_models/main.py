import os
from pathlib import Path

from trilogy import Dialects, Executor
from trilogy.dialect import DuckDBConfig, MySQLConfig, SQLiteConfig

from trilogy_public_models.discovery import data_models
from trilogy_public_models.models import LazyEnvironment, QueryType


def get_executor(
    model: str, executor: Executor | None = None, run_setup: bool = True
) -> Executor:
    conf: DuckDBConfig | MySQLConfig | SQLiteConfig | None = None
    if "bigquery" in model:
        dialect = Dialects.BIGQUERY
    elif "duckdb" in model:
        dialect = Dialects.DUCK_DB
        conf = DuckDBConfig(enable_gcs=True, enable_python_datasources=True)
    elif "snowflake" in model:
        dialect = Dialects.SNOWFLAKE
    elif "sqlite" in model:
        dialect = Dialects.SQLITE
        model_dir = Path(__file__).parent / model.replace(".", "/")
        db_files = sorted(model_dir.glob("*.db"))
        if db_files:
            conf = SQLiteConfig(path=str(db_files[0]))
    elif "mysql" in model:
        dialect = Dialects.MYSQL
        database = model.removeprefix("mysql.beaver_")
        conf = MySQLConfig(
            host=os.environ.get("BEAVER_MYSQL_HOST", "127.0.0.1"),
            port=int(os.environ.get("BEAVER_MYSQL_PORT", "3306")),
            username=os.environ.get("BEAVER_MYSQL_USER", "root"),
            password=os.environ.get("BEAVER_MYSQL_PASSWORD", "beaver"),
            database=database,
        )
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
