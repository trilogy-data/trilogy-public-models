def test_import():
    from trilogy_public_models import models

    from trilogy_public_models.bigquery import stack_overflow

    assert models["bigquery.stack_overflow"].concepts == stack_overflow.concepts
