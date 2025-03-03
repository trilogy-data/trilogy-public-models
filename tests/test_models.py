from trilogy_public_models import data_models, get_executor
from trilogy_public_models.validator import validate_model
from concurrent.futures import ThreadPoolExecutor
from trilogy import Environment

SKIPPED_KEYS = ["bigquery.age_of_empires_2", "duckdb.titanic"]


def single_model(key, model: Environment, bq_executor, bq_client):
    try:
        validate_model(model, bq_executor, bq_client)
    except Exception as e:
        raise ValueError(
            f"Failed to handle model {key} {type(model)} with error {str(e)}"
        )


def test_models(bq_client, bq_executor):
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
            else:
                raise NotImplementedError(f"Model {key} not supported")
    for future in results:
        result = future.result()
        print(result)
