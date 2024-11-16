def test_import():
    from trilogy_public_models import data_models

    from trilogy_public_models.bigquery import stack_overflow

    assert (
        data_models["bigquery.stack_overflow"].environment.concepts
        == stack_overflow.concepts
    )
