from os import listdir
from os.path import dirname, join

from preql import Environment
from preql.parser import parse


def parse_initial_models(fpath: str) -> Environment:
    files = listdir(dirname(fpath))

    for file in files:
        if file.endswith("entrypoint.preql"):
            with open(join(dirname(fpath), file), "r", encoding="utf-8") as f:
                contents = f.read()
                env = Environment(working_path=dirname(fpath))
                environment, statements = parse(contents, environment=env)
                return environment
    raise ValueError("Missing entrypoint.preql")
