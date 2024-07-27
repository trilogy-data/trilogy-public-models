import pkgutil
from trilogy import Environment
from os.path import dirname
from pathlib import Path
import sys
from importlib.machinery import SourceFileLoader
from trilogy.core.models import LazyEnvironment
from collections import UserDict


class ModelDict(UserDict[str, Environment]):
    def __init__(self):
        super().__init__()
        self.not_exists: set[str] = set()

    def __getitem__(self, item: str):
        path = str(Path(__file__).parent / item.replace(".", "/") / "__init__.py")
        if item not in self and item not in self.not_exists:
            # imports the module from the given path
            loaded = SourceFileLoader(item, path).load_module()
            self[item] = loaded.model
            sys.modules["trilogy_public_models." + item] = loaded.model
            return loaded.model
        response = super().__getitem__(item)
        # if the key is set but not loaded yet
        if not response:
            loaded = SourceFileLoader(item, path).load_module()
            self[item] = loaded.model
            sys.modules["trilogy_public_models." + item] = loaded.model
            response = loaded.model
        return response

    def __setitem__(self, key, value):
        super().__setitem__(key, value)


models: UserDict["str", Environment] = ModelDict()

__version__ = "0.0.21"


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


def force_load_all():
    for info in pkgutil.walk_packages([dirname(__file__)]):
        try:
            load_module_wrap(info)
        # this is expected in pyinstaller packages
        except AttributeError:
            pass


def discover_models():
    import os

    base = dirname(__file__)

    for root, dirs, files in os.walk(base):
        for f in files:
            if f == "entrypoint.preql":
                relative = Path(root).relative_to(base)
                path = ".".join(relative.parts)
                models[path] = None
                sys.modules["trilogy_public_models." + path] = LazyEnvironment(
                    load_path=Path(root) / f, working_path=Path(root)
                )


discover_models()

__all__ = ["models"]
