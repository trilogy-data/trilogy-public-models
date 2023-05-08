import pkgutil
from preql import Environment
from typing import Dict
from os.path import dirname
import sys

models: Dict["str", Environment] = {}

__version__ = "0.0.8"

for loader, module_name, is_pkg in pkgutil.walk_packages([dirname(__file__)]):
    module = loader.find_module(module_name)  # type: ignore
    if not module:
        continue
    _module = module.load_module(module_name)
    try:
        sys.modules["trilogy_public_models." + module_name] = _module.model
        models[module_name] = _module.model
    except AttributeError:
        continue

__all__ = ["models"]
