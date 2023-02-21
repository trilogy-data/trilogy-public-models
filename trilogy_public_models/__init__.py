import pkgutil
from preql import Environment
from typing import Dict
from os.path import dirname
import sys

models: Dict["str", Environment] = {}

__version__ = "0.0.1"

for loader, module_name, is_pkg in pkgutil.walk_packages([dirname(__file__)]):
    _module = loader.find_module(module_name).load_module(module_name)
    try:
        sys.modules[module_name] = _module.model
        models[module_name] = _module.model
    except AttributeError:
        continue

__all__ = ['models']