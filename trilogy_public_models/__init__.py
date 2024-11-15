import pkgutil
from trilogy import Environment
from os.path import dirname
from pathlib import Path
import sys
from typing import Any
from importlib.machinery import SourceFileLoader
from trilogy_public_models.models import LazyEnvironment, ModelOutput, ModelDict
from trilogy_public_models.main import get_executor
from trilogy_public_models.discovery import discover_models
from collections import UserDict
from dataclasses import dataclass


data_models: UserDict["str", ModelOutput] = ModelDict()

__version__ = "0.0.22"

discover_models(data_models)

__all__ = ["data_models", "get_executor"]
