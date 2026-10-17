"""Models read through public Query.Farm VGI workers.

Only haybarn's DuckDB can load the vgi extension, so these run in their own CI
job (``vgi`` in pythonpackage.yml), which sets REQUIRE_HAYBARN so a missing
install fails instead of skipping. Elsewhere they skip without haybarn.
"""

import os
from copy import deepcopy

import pytest

from tests.test_examples import _extract_queries
from trilogy_public_models import data_models, get_executor
from trilogy_public_models.main import is_vgi_model
from trilogy_public_models.validator import example_path, validate_model, validate_query

VGI_MODELS = sorted(key for key in data_models if is_vgi_model(key))

# These answer only bounded requests, so the unfiltered scans of model
# validation cannot be served: open_meteo's forecasts are table functions of a
# coordinate the query must pin, overture refuses a scan without a bounding
# box, and the USGS catalog is the full ComCat history. Their examples bound
# every read.
BOUNDED_MODELS = {
    "duckdb.open_meteo",
    "duckdb.overture_places",
    "duckdb.usgs_earthquakes",
}


@pytest.fixture(scope="module", autouse=True)
def haybarn():
    if os.environ.get("REQUIRE_HAYBARN"):
        import haybarn  # noqa: F401

        return
    pytest.importorskip("haybarn")


def test_bounded_models_are_vgi_models():
    assert BOUNDED_MODELS <= set(VGI_MODELS)


@pytest.mark.parametrize("key", VGI_MODELS)
def test_vgi_model(key: str):
    executor = get_executor(key)
    if key not in BOUNDED_MODELS:
        validate_model(key, data_models[key].environment, executor, None)
    engine, name = key.split(".")
    queries = list(_extract_queries(example_path / engine / name))
    assert queries, f"{key} has no examples"
    environment = deepcopy(data_models[key].environment)
    for _, query in queries:
        validate_query(query, environment, executor, None)
