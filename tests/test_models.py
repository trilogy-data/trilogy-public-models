from trilogy_public_models import data_models, get_executor
from trilogy_public_models.validator import validate_model
from concurrent.futures import ThreadPoolExecutor
from trilogy import Environment
import traceback
from pydantic.errors import PydanticUserError


SKIPPED_KEYS = ["bigquery.age_of_empires_2", "duckdb.titanic"]


def single_model(key, model: Environment, bq_executor, bq_client, retry: bool = False):
    try:
        validate_model(key, model, bq_executor, bq_client)
    except PydanticUserError as e:
        if retry:
            raise e
        return single_model(key, model, bq_executor, bq_client, True)
    except Exception as e:
        error_traceback = traceback.format_exc()
        raise ValueError(
            f"Failed to handle model {key} {type(model)} with error:\n{str(e)} from \n{error_traceback}"
        )


def test_models(bq_client, bq_executor, snowflake_executor):
    results = []
    with ThreadPoolExecutor() as executor:
        for key, model in data_models.items():
            if key in SKIPPED_KEYS:
                continue
            if "bigquery" in key:
                trilogy_executor = get_executor(key, executor=bq_executor())
                future = executor.submit(
                    single_model, key, model.environment, trilogy_executor, bq_client()
                )
                results.append(future)
            elif "duckdb" in key:
                trilogy_executor = get_executor(key)
                future = executor.submit(
                    single_model, key, model.environment, trilogy_executor, None
                )
                results.append(future)
            elif "snowflake" in key:
                trilogy_executor = get_executor(key, executor=snowflake_executor())
                future = executor.submit(
                    single_model, key, model.environment, trilogy_executor, None
                )
                results.append(future)
            else:
                raise NotImplementedError(f"Model {key} not supported")
    for future in results:
        result = future.result()
        print(result)
