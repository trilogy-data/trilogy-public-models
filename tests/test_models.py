import os
import traceback
from collections.abc import Iterable

import pytest
from pydantic.errors import PydanticUserError
from trilogy import Environment

from trilogy_public_models import data_models, get_executor
from trilogy_public_models.validator import validate_model

# duckdb.covid19_open_data reads its parquet tiles from GCS (not committed to
# git); they are published by the `data/` trilogy-cloud refresh job. It is
# validated in CI by a dedicated build + `trilogy integration` step against
# locally-built tiles (see .github/workflows/pythonpackage.yml).
SKIPPED_KEYS = [
    # No public tiles yet; covered by offline test_danish_democracy.py until
    # bootstrap and deletion reconciliation are approved (see model README).
    "duckdb.danish_democracy",
    "duckdb.covid19_open_data",
    # The only BigQuery model on a private project: it reads
    # ttl-test-355422.aoe2, while every other one reads bigquery-public-data.
    # CI authenticates as the federated preqldata service account, which holds
    # no IAM on that dataset, so every query 403s ("User does not have
    # permission to query table ttl-test-355422:aoe2.match_player_actions").
    # It validates for a principal that can read it; grant that SA access to
    # drop this skip.
    "bigquery.age_of_empires_2",
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


def backend_of(key: str) -> str:
    return key.split(".", 1)[0]


def model_params(keys: Iterable[str]) -> list:
    """One test per model, marked with its backend so CI can split the suite."""
    return [
        pytest.param(key, id=key, marks=getattr(pytest.mark, backend_of(key)))
        for key in keys
    ]


def executor_for(key: str, request: pytest.FixtureRequest):
    """The model's executor and, for BigQuery, its dry-run client. Fixtures are
    resolved per backend, so a DuckDB run needs no Google credentials."""
    backend = backend_of(key)
    if backend == "bigquery":
        executor = request.getfixturevalue("bq_executor")()
        return get_executor(key, executor=executor), request.getfixturevalue(
            "bq_client"
        )()
    if backend == "snowflake":
        executor = request.getfixturevalue("snowflake_executor")()
        return get_executor(key, executor=executor), None
    if backend in ("duckdb", "sqlite"):
        return get_executor(key), None
    raise NotImplementedError(f"Model {key} not supported")


@pytest.mark.parametrize(
    "key", model_params(k for k in data_models if k not in SKIPPED_KEYS)
)
def test_model(key: str, request: pytest.FixtureRequest):
    executor, dry_run_client = executor_for(key, request)
    single_model(key, data_models[key].environment, executor, dry_run_client)
