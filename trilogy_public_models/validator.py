from trilogy import Environment, Dialects
from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.authoring import (
    Concept,
    ConceptRef,
    SelectStatement,
)
from trilogy.core.models.datasource import Datasource
from trilogy.core.statements.execute import ProcessedShowStatement
from trilogy.core.processing.concept_strategies_v3 import search_concepts, History
from trilogy.executor import Executor
from trilogy.parser import parse_text
from trilogy.core.internal import INTERNAL_NAMESPACE
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import Factory, BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment


def safe_address(input: Concept | ConceptRef):
    if input.namespace == DEFAULT_NAMESPACE:
        return input.name
    return input.address


def validate_dataset(
    dataset: Datasource, environment: Environment, executor: Executor, dry_run_client
):

    validation_query = (
        "SELECT\n "
        + ",\n\t".join([safe_address(x) for x in dataset.concepts])
        + " LIMIT 0;"
    )

    try:
        _, parsed = parse_text(validation_query, environment)
        processed: list[SelectStatement] = [
            x for x in parsed if isinstance(x, SelectStatement)
        ]
        sql = executor.generator.generate_queries(environment, processed)
    except Exception as e:
        print(validation_query)
        raise e
    for statement in sql:
        if isinstance(statement, ProcessedShowStatement):
            continue
        compiled_sql = ""
        # Start the query, passing in the extra configuration.
        try:
            # for UI execution, cap the limit
            compiled_sql = executor.generator.compile_statement(statement)

            # TODO: implement this
            # rs = executor.engine.dry_run(compiled_sql)
            if executor.dialect == Dialects.BIGQUERY:
                # use a dry run to save costs
                # TODO: move ths into executor so we don't need this import
                from google.cloud.bigquery import QueryJobConfig

                job_config = QueryJobConfig(dry_run=True, use_query_cache=False)
                query_job = dry_run_client.query(
                    compiled_sql, job_config=job_config
                )  # Make an API request.
                print(
                    "This query will process {} bytes.".format(
                        query_job.total_bytes_processed
                    )
                )
            elif executor.dialect == Dialects.DUCK_DB:
                # use a dry run to save costs
                query_job = executor.execute_raw_sql(compiled_sql)
            else:
                raise NotImplementedError(
                    f"Validation not implemented for {executor.dialect}"
                )
        except Exception as e:
            print("Failed validation on:")
            print(validation_query)
            print(compiled_sql)
            raise e


def validate_datasource_grain(datasource):
    # TODO: check that count(*) at datasource grain = 1
    # CON: this requires running an real query
    pass


def validate_concept(concept: BuildConcept, history: History, env, graph):
    if concept.namespace == INTERNAL_NAMESPACE or INTERNAL_NAMESPACE in concept.address:
        return
    search_concepts([concept], history=history, environment=env, depth=0, g=graph)


def validate_model(model: Environment, executor: Executor, dry_run_client):
    for dataset in model.datasources.values():
        validate_dataset(dataset, model, executor, dry_run_client)
    history = History(base_environment=model)
    factory = Factory(model)
    build_model: BuildEnvironment = factory.build(model)
    graph = generate_graph(build_model)
    for concept in build_model.concepts.values():
        validate_concept(concept, history, build_model, graph)
