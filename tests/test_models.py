import os
import traceback
from concurrent.futures import ThreadPoolExecutor

from pydantic.errors import PydanticUserError
from trilogy import Environment

from trilogy_public_models import data_models, get_executor
from trilogy_public_models.validator import validate_model

# duckdb.covid19_open_data reads its parquet tiles from GCS (not committed to
# git); they are published by the Refresh Data workflow on merge to main. It is
# validated in CI by a dedicated build + `trilogy integration` step against
# locally-built tiles (see .github/workflows/pythonpackage.yml).
SKIPPED_KEYS = [
    "bigquery.age_of_empires_2",
    "duckdb.titanic",
    "duckdb.covid19_open_data",
    # BEAVER's MySQL dump is prepared locally and is not available in CI.
    "mysql.beaver_dw",
    "mysql.beaver_neutron",
    "mysql.beaver_nova",
]

# duckdb.layercake reads planet-scale OSM parquet hosted by OpenStreetMap US;
# its grain checks stream ~2GB (the type/id columns of the 700M-row buildings
# and 300M-row highways layers) over HTTP and aggregate ~1B groups. That is too
# slow and bandwidth-heavy for CI, and heavy enough to take down a developer
# machine, so it is skipped by default everywhere rather than just in CI. Set
# VALIDATE_LAYERCAKE=1 to opt in when you intend to pay that cost.
if not os.environ.get("VALIDATE_LAYERCAKE"):
    SKIPPED_KEYS.append("duckdb.layercake")


def single_model(key, model: Environment, bq_executor, bq_client, retry: bool = False):
    try:
        validate_model(key, model, bq_executor, bq_client)
    except PydanticUserError:
        if retry:
            raise
        return single_model(key, model, bq_executor, bq_client, True)
    except Exception as e:
        error_traceback = traceback.format_exc()
        raise ValueError(
            f"Failed to handle model {key} {type(model)} with error:\n{e!s} from \n{error_traceback}"
        ) from e


def test_models(bq_client, bq_executor, snowflake_executor):
    results = []
    with ThreadPoolExecutor() as executor:
        for key, model in data_models.items():
            print(f"Validating {key}")
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
            elif "sqlite" in key:
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
