from trilogy.core.models import Environment
from pathlib import Path
from typing import Any, Callable
import pkgutil
from trilogy import Environment
from os.path import dirname
from pathlib import Path
import sys
from typing import Any
from importlib.machinery import SourceFileLoader
from collections import UserDict
from dataclasses import dataclass




@dataclass
class ModelOutput:
    environment: Environment
    setup: list[Any]

class ModelDict(UserDict[str, Environment]):
    def __init__(self):
        super().__init__()
        self.not_exists: set[str] = set()
        

    def __getitem__(self, item: str)->ModelOutput:
        path = str(Path(__file__).parent / item.replace(".", "/") / "__init__.py")
        if item not in self and item not in self.not_exists:
            # imports the module from the given path
            loaded = SourceFileLoader(item, path).load_module()
            output = ModelOutput(environment=loaded.model, setup= loaded.statements)
            self[item] = output
            sys.modules["trilogy_public_models." + item] = output
            return output
        response = super().__getitem__(item)
        # if the key is set but not loaded yet
        if not response:
            loaded = SourceFileLoader(item, path).load_module()
            output = ModelOutput(environment=loaded.model, setup= loaded.statements)
            self[item] = output
            sys.modules["trilogy_public_models." + item] = output
            response = output
        return response

    def __setitem__(self, key, value):
        super().__setitem__(key, value)

class LazyEnvironment(Environment):
    """Variant of environment to defer parsing of a path
    until relevant attributes accessed."""

    load_path: Path
    working_path: Path
    setup_queries: list[Any]

    def __getattribute__(self, name):
        if name in (
            "load_path",
            "loaded",
            "setup_queries",
            "working_path",
            "model_config",
            "model_fields",
            "model_post_init",
        ) or name.startswith("_"):
            return super().__getattribute__(name)
        if not self.loaded:
            from trilogy import parse

            env = Environment(working_path=str(self.working_path))
            with open(self.load_path, "r") as f:
                env, q = parse(f.read(), env)
                self.setup_queries += q
            self.loaded = True
            self.datasources = env.datasources
            self.concepts = env.concepts
            self.imports = env.imports
            self.alias_origin_lookup = env.alias_origin_lookup
            self.materialized_concepts = env.materialized_concepts
            self.functions = env.functions
            self.data_types = env.data_types
            self.cte_name_map = env.cte_name_map
        return super().__getattribute__(name)