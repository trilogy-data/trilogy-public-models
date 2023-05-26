from trilogy_public_models import models
from trilogy_public_models.validator import validate_model


def test_models(bq_client, bq_executor):
    for key, model in models.items():
        try:
            validate_model(model, bq_executor, bq_client)
        except Exception as e:
            raise ValueError(f'Failed to handle model {key} with error {str(e)}')
