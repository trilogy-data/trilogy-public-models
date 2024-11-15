from trilogy_public_models import data_models
from trilogy import Executor, Dialects


def get_executor(model:str, executor:Executor | None = None)->Executor:

    if 'bigquery' in model:
        dialect = Dialects.BIGQUERY
    elif 'duckdb' in model:
        dialect = Dialects.DUCK_DB
    else:
        raise NotImplementedError(f"Model {model} not supported")
    loaded = data_models[model]
    if executor is None:
        executor = dialect.default_executor(environment=loaded.environment)
    else:
        executor.environment = loaded.environment
    for x in loaded.setup:
        executor.execute_statement(x)
    return executor