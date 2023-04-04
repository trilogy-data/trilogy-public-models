# trilogy-public-models

## Overview

This repository contains semantic models on public datasets for the Preql/Trilogy language. 

This supports the interactive PreQL demo, but can also be used by anyone to boostrap exploration
of these public datasets.

You can install this library directly and import models to use.

## Installation

```commandline
pip install trilogy-public-models
```

## Examples

This repository also contains a examples/ folder, which can be browsed for in-depth code examples.

## Local Usage

This example assumes you are querying Bigquery Datasets.

To utilize a model, instantiate a standard PreQL executor (in this case, a bigquery client) 
and then pass in one of the existing environments from this package into the environment argument.

That will enable you to run queries against the semantic model.

```python
from google.auth import default
from google.cloud import bigquery
from preql.executor import Executor, Dialects
from sqlalchemy.engine import create_engine

from trilogy_public_models.bigquery import google_search_trends

project, auth = default()
bq_client = bigquery.Client(auth, project)

engine = create_engine(f"bigquery://{project}?user_supplied_client=True",
                       connect_args={'client': bq_client})

exec = Executor(
    dialect=Dialects.BIGQUERY, engine=engine,
    environment=google_search_trends
)

results = exec.execute_text("""
SELECT 
	trends.term,
	trends.rank,
	trends.week,
	trends.refresh_date,
WHERE
    trends.week > '2023-01-01'
    and trends.refresh_date = '2023-02-22'
    and trends.rank < 10
ORDER BY 
    trends.week desc,
    trends.rank asc
limit 100;

""")

# you can execute multiple queries separate by a semicolon
# so our results will be in the first element of the arra
for row in results[0]:
    print(row)


```

## Combining Models

Coming soon!

Preql supports combining multiple environments into a single environment. This enables simplified querying
of universal concepts, like looking up StackOverflow links embedded in Github commits, or merging GPS
data across different domains. 

## Contributing

### Model setup

All models should be in a double nested directory; first the platform and then the semantic label of the model

Models should have the following

- entrypoint.preql
- README.md


### Model Tests

All models will be imported and verified. Validation methods will depend on the defined backend. 

All models require that the datasets being shared with the preql validation account. 

Current verifications:

 - model imports successfully
 - datasource bindings exist
 - datasource to concept mappings are appropriately typed
 - concept relations are consistently typed
