from trilogy_public_models import models
from trilogy_public_models.validator import validate_model
from concurrent.futures import ThreadPoolExecutor


def single_model(key, model, bq_executor, bq_client):
    # permission issue with this
    if key == "bigquery.age_of_empires_2":
        return
    try:
        validate_model(model, bq_executor, bq_client)
    except Exception as e:
        raise ValueError(
            f"Failed to handle model {key} {type(model)} with error {str(e)}"
        )


def test_models(bq_client, bq_executor):
    results = []
    with ThreadPoolExecutor() as executor:
        for key, model in models.items():
            future = executor.submit(
                single_model, key, model, bq_executor(), bq_client()
            )
            results.append(future)

    for future in results:
        result = future.result()
        print(result)
