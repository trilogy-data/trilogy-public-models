import pkgutil
from preql import Environment
from typing import Dict
from os.path import dirname
from pathlib import Path
import sys
from importlib.machinery import SourceFileLoader


class ModelDict(Dict[str, Environment]):
    def __init__(self):
        super().__init__()
        self.not_exists: set[str] = set()

    def __getitem__(self, item: str):
        path = str(Path(__file__).parent / item.replace(".", "/") / "__init__.py")
        if item not in self and item not in self.not_exists:
            # imports the module from the given path
            loaded = SourceFileLoader(item, path).load_module()
            self[item] = loaded.model
            globals()[item] = loaded.model
            sys.modules["trilogy_public_models." + item] = loaded.model
            return loaded.model
        return super().__getitem__(item)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)


models: Dict["str", Environment] = ModelDict()

__version__ = "0.0.16"


def load_module_wrap(info: pkgutil.ModuleInfo):
    loader, module_name, is_pkg = info
    module = loader.find_module(module_name)  # type: ignore
    if not module:
        return None
    try:
        sys.modules[ # type: ignore
            "trilogy_public_models." + module_name
        ] = module.load_module(module_name).model
    except AttributeError:
        return None


def force_load_all():
    for info in pkgutil.walk_packages([dirname(__file__)]):
        try:
            load_module_wrap(info)
        # this is expected in pyinstaller packages
        except AttributeError:
            pass


__all__ = ["models"]
