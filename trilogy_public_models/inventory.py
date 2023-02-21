from os import listdir
from os.path import dirname, join
from pathlib import Path
from typing import Dict

from preql import Environment
from preql.parser import parse


def parse_initial_models(fpath: str) -> Dict[str, Environment]:
    output = {}
    files = listdir(dirname(fpath))

    for file in files:
        path = Path(file)
        if file.endswith("entrypoint.preql"):
            model_name = path.parts[-1].split(".")[0]
            with open(join(dirname(fpath), file), "r", encoding="utf-8") as f:
                contents = f.read()
                env = Environment(working_path=dirname(fpath))
                environment, statements = parse(contents, environment=env)
                return environment
