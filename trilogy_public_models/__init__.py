import pkgutil
from preql import Environment
from typing import Dict
from os.path import dirname
import sys

models: Dict["str", Environment] = {}

__version__ = "0.0.16"


def load_module_wrap(info: pkgutil.ModuleInfo):
    loader, module_name, is_pkg = info
    module = loader.find_module(module_name)  # type: ignore
    if not module:
        return None
    _module = module.load_module(module_name)
    try:
        sys.modules["trilogy_public_models." + module_name] = _module.model
        models[module_name] = _module.model
    except AttributeError:
        return None


for info in pkgutil.walk_packages([dirname(__file__)]):
    try:
        load_module_wrap(info)
    # this is expected in pyinstaller packages
    except AttributeError:
        pass

__all__ = ["models"]
