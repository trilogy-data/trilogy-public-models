import pkgutil
from trilogy import Environment
from os.path import dirname
from pathlib import Path
import sys
from typing import Any
from importlib.machinery import SourceFileLoader
from trilogy_public_models.models import LazyEnvironment, ModelOutput, ModelDict
from trilogy_public_models.main import get_executor
from collections import UserDict
from dataclasses import dataclass

def load_module_wrap(info: pkgutil.ModuleInfo):
    loader, module_name, is_pkg = info
    module = loader.find_module(module_name)  # type: ignore
    
    if not module:
        return None
    loaded = module.load_module(module_name)
    output = ModelOutput(environment=loaded.model, setup= loaded.statements)
    try:
        sys.modules["trilogy_public_models." + module_name] = output
        models[module_name] = output
    except AttributeError:
        return None


def force_load_all():
    for info in pkgutil.walk_packages([dirname(__file__)]):
        try:
            load_module_wrap(info)
        # this is expected in pyinstaller packages
        except AttributeError:
            pass


def discover_models(models):
    import os

    base = dirname(__file__)

    for root, dirs, files in os.walk(base):
        for f in files:
            if f == "entrypoint.preql":
                relative = Path(root).relative_to(base)
                path = ".".join(relative.parts)
                models[path] = None
                lazy_env = LazyEnvironment(
                    load_path=Path(root) / f, working_path=Path(root),
                    setup_queries= []

                )
                output = ModelOutput(environment=lazy_env, setup = lazy_env.setup_queries)
                sys.modules["trilogy_public_models." + path] = output
                models[path] = output

