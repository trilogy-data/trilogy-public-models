from os.path import dirname
from pathlib import Path
import sys
from trilogy_public_models.models import LazyEnvironment, ModelOutput, ModelDict
from collections import UserDict


def discover_models(models):
    import os

    base = dirname(__file__)

    for root, dirs, files in os.walk(base):
        for f in files:
            if f == "entrypoint.preql":
                relative = Path(root).relative_to(base)
                path = ".".join(relative.parts)
                models[path] = None
                query_array = []
                lazy_env = LazyEnvironment(
                    load_path=Path(root) / f,
                    working_path=Path(root),
                    setup_queries=query_array,
                )
                output = ModelOutput(environment=lazy_env, setup=lazy_env.setup_queries)
                sys.modules["trilogy_public_models." + path] = output.environment
                models[path] = output


data_models: UserDict["str", ModelOutput] = ModelDict()
discover_models(data_models)
