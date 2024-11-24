from pathlib import Path
from typing import Any
from pydantic import BaseModel
from trilogy import Environment
import sys
from importlib.machinery import SourceFileLoader
from collections import UserDict
from dataclasses import dataclass


@dataclass
class ModelOutput:
    environment: Environment
    setup: list[Any]


class ModelDict(UserDict[str, ModelOutput]):
    def __init__(self):
        super().__init__()
        self.not_exists: set[str] = set()

    def _load(self, item: str, path: str):
        loaded = SourceFileLoader(item, path).load_module()
        output = ModelOutput(environment=loaded.model, setup=loaded.statements)
        self[item] = output
        sys.modules["trilogy_public_models." + item] = output  # type: ignore
        return output

    def __getitem__(self, item: str) -> ModelOutput:
        path = str(Path(__file__).parent / item.replace(".", "/") / "__init__.py")
        if item not in self and item not in self.not_exists:
            return self._load(item, path)
        response = super().__getitem__(item)
        # if the key is set but not loaded yet
        if not response:
            return self._load(item, path)
        return response

    def __setitem__(self, key, value):
        super().__setitem__(key, value)


class LazyEnvironment(Environment):
    """Variant of environment to defer parsing of a path
    until relevant attributes accessed."""

    load_path: Path

    working_path: Path
    setup_queries: list[Any]
    loaded: bool = False

    def __init__(self, **data):
        # skip the Environment ini
        # as it will be called late
        BaseModel.__init__(self, **data)

    @property
    def setup_path(self) -> Path:
        return self.load_path.parent / "setup.preql"

    def _load(self):
        if self.loaded:
            return
        from trilogy import parse

        env = Environment(working_path=str(self.working_path))

        with open(self.load_path, "r") as f:
            env, _ = parse(f.read(), env)
        if self.setup_path.exists():
            with open(self.setup_path, "r") as f2:
                env, q = parse(f2.read(), env)
                for q in q:
                    self.setup_queries.append(q)
        self.loaded = True
        self.datasources = env.datasources
        self.concepts = env.concepts
        self.imports = env.imports
        self.alias_origin_lookup = env.alias_origin_lookup
        self.materialized_concepts = env.materialized_concepts
        self.functions = env.functions
        self.data_types = env.data_types
        self.cte_name_map = env.cte_name_map

    def __getattribute__(self, name):
        if name not in (
            "datasources",
            "concepts",
            "imports",
            "materialized_concepts",
            "functions",
            "datatypes",
            "cte_name_map",
        ) or name.startswith("_"):
            return super().__getattribute__(name)
        if not self.loaded:
            self._load()
        return super().__getattribute__(name)
