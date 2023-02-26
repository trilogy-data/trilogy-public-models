from trilogy_public_models import models
from trilogy_public_models.validator import validate_model


def test_models(bq_client, bq_executor):
    for key, model in models.items():
        validate_model(model, bq_executor, bq_client)
