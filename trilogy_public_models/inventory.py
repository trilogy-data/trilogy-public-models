from os import listdir
from os.path import dirname, join

from trilogy import Environment
from trilogy.constants import ENV_CACHE_NAME
from trilogy.parser import parse


def parse_initial_models(fpath: str) -> Environment:
    parent_folder = dirname(fpath)
    files = listdir(dirname(fpath))
    cache_path = join(parent_folder, ENV_CACHE_NAME)
    # 2024-03-31 - disable due to issue with pydantic parsing from cache wrong
    # for file in files:
    #     if file == ENV_CACHE_NAME:
    #         env = Environment.from_cache(join(parent_folder, file))
    #         if env:
    #             return env
    for file in files:
        if file.endswith("entrypoint.preql"):
            with open(join(parent_folder, file), "r", encoding="utf-8") as f:
                contents = f.read()
                env = Environment(working_path=parent_folder)
                environment, statements = parse(contents, environment=env)
                env.to_cache(cache_path)
                return environment
    raise ValueError("Missing entrypoint.preql")
