from preql import Environment
from preql.core.query_processor import get_datasource_by_concept_and_grain
from preql.core.models import Grain, Concept, Datasource
from preql.executor import Executor
from preql.parser import parse_text


def validate_dataset(dataset:Datasource, environment:Environment, executor:Executor, dry_run_client):
    # TODO: move ths into executor so we don't need this import
    from google.cloud.bigquery import QueryJobConfig
    validation_query = 'SELECT\n '+ ',\n'.join([x .address for x in dataset.concepts]) +' LIMIT 0;'

    _, parsed = parse_text(validation_query, environment)
    sql = executor.generator.generate_queries(environment, parsed)
    print(validation_query)
    for statement in sql:
        # for UI execution, cap the limit
        compiled_sql = executor.generator.compile_statement(statement)
        print(compiled_sql)
        #TODO: implement this
        #rs = executor.engine.dry_run(compiled_sql)
        job_config = QueryJobConfig(dry_run=True, use_query_cache=False)

        # Start the query, passing in the extra configuration.
        query_job = dry_run_client.query(compiled_sql,
            job_config=job_config,
        )  # Make an API request.

        #TODO: do something interesting with cost modeling?
        #
        print("This query will process {} bytes.".format(query_job.total_bytes_processed))




def validate_concept(concept:Concept, env):
    get_datasource_by_concept_and_grain(concept, grain=Grain(components=[concept.with_default_grain()]),
                                        environment=env)



def validate_model(model:Environment, executor:Executor, dry_run_client):
    for dataset in model.datasources.values():
        validate_dataset(dataset, model, executor, dry_run_client)
    for concept in model.concepts.values():
        validate_concept(concept, model)
