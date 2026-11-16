from json import loads
from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.authoring import SelectStatement
from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Datasource
from trilogy.core.statements.execute import ProcessedShowStatement
from trilogy.executor import Executor
from trilogy.parser import parse_text

example_path = Path(__file__).parent.parent / "examples"


def validate_query(
    validation_query: str, environment: Environment, executor: Executor, dry_run_client
):
    environment.cte_name_map = {}
    try:
        _, parsed = parse_text(validation_query, environment)
        processed: list[SelectStatement] = [
            x for x in parsed if isinstance(x, SelectStatement)
        ]
        sql = executor.generator.generate_queries(environment, processed)
    except Exception:
        print("Failing Validation Query Is")
        print(validation_query)
        raise
    for statement in sql:
        if isinstance(statement, ProcessedShowStatement):
            continue
        compiled_sql = ""
        try:
            compiled_sql = executor.generator.compile_statement(statement)
            check_sql(compiled_sql, executor, dry_run_client)
        except Exception:
            print("Failed validation on:")
            print(validation_query)
            print(compiled_sql)
            raise


def check_sql(compiled_sql: str, executor: Executor, dry_run_client) -> None:
    """Have the warehouse check a query, without paying to run it where it can."""
    if executor.dialect == Dialects.BIGQUERY:
        # a dry run is free
        from google.cloud.bigquery import QueryJobConfig

        job_config = QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = dry_run_client.query(compiled_sql, job_config=job_config)
        print(f"This query will process {query_job.total_bytes_processed} bytes.")
    elif executor.dialect in (Dialects.DUCK_DB, Dialects.SQLITE, Dialects.SNOWFLAKE):
        # local files; snowflake runs against fakesnow
        executor.execute_raw_sql(compiled_sql)
    elif executor.dialect == Dialects.DATABRICKS:
        # EXPLAIN analyzes the query against the catalog without scanning data;
        # a planning error comes back as the plan text
        plan = executor.execute_raw_sql(f"EXPLAIN {compiled_sql}").fetchall()
        if plan and str(plan[0][0]).startswith("Error occurred"):
            raise ValueError(f"Databricks rejected the query: {str(plan[0][0])[:1000]}")
    else:
        raise NotImplementedError(f"Validation not implemented for {executor.dialect}")


def validate_datasource_columns(
    datasource: Datasource, executor: Executor, dry_run_client
) -> None:
    """Every physical column a table datasource maps exists in its table.

    Examples only read the columns they use, so a misspelled column elsewhere
    would pass them. Selects the mapped columns directly, with no planning.
    """
    address = datasource.address
    if isinstance(address, str) or address.type != AddressType.TABLE:
        return
    columns = [c.alias for c in datasource.columns if isinstance(c.alias, str)]
    if not columns:
        return
    # safe_quote quotes each part of a dotted path to a nested field
    quote = executor.generator.safe_quote
    sql = (
        f"SELECT {', '.join(quote(c) for c in columns)}"
        f" FROM {executor.generator.safe_quote(address.location)} LIMIT 0"
    )
    try:
        check_sql(sql, executor, dry_run_client)
    except Exception as e:
        raise ValueError(f"Datasource {datasource.name} failed validation: {e}") from e


def validate_datasource_grain(datasource):
    # TODO: check that count(*) at datasource grain = 1
    # CON: this requires running an real query
    pass


def get_example_queries(key: str) -> list[str]:
    type, name = key.split(".")
    example_dir = example_path / type / name
    queries = example_dir.glob("*.preql")
    final = []
    for query in queries:
        with open(query, "r") as f:
            final.append(f.read())
    dashboards = example_dir.glob("*.json")
    for dashboard in dashboards:
        with open(dashboard, "r") as f:
            content: dict = loads(f.read())
            imports = content.get("imports", [])
            imp_prefix = " ".join([f"import {x['name']};" for x in imports])
            for v in content["gridItems"].values():
                content = v.get("content", None)
                if content and isinstance(content, dict) and content.get("query", None):
                    final.append(imp_prefix + " " + content["query"])
                elif v.get("type") in ["chart", "table"] and v.get("content", None):
                    final.append(imp_prefix + " " + v["content"])

    return final


def validate_model(key: str, model: Environment, executor: Executor, dry_run_client):
    if executor.dialect == Dialects.DUCK_DB:
        return executor.validate_environment()
    for datasource in unique_datasources(model):
        validate_datasource_columns(datasource, executor, dry_run_client)
    for example in get_example_queries(key):
        validate_query(example, model, executor, dry_run_client)


def unique_datasources(model: Environment) -> list[Datasource]:
    """One datasource per physical table: an imported file shows up once per
    import path."""
    seen: dict[tuple, Datasource] = {}
    for datasource in model.datasources.values():
        cols = tuple(c.alias for c in datasource.columns if isinstance(c.alias, str))
        seen.setdefault((str(datasource.address), cols), datasource)
    return list(seen.values())
