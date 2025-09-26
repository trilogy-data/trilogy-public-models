from trilogy.core.validation.fix import validate_and_rewrite
from trilogy.core.validation.environment import validate_environment
from pathlib import Path
from trilogy import Dialects, Environment

DUCKDB_MODELS_PATH = Path(__file__).parent.parent / "trilogy_public_models" / "duckdb"


def process_model(file_path: Path):
    # find the setup file
    engine = Dialects.DUCK_DB.default_executor()
    found_setup = False
    for setup_file in file_path.glob("setup.sql"):
        found_setup = True
        with open(setup_file, "r") as f:
            setup_sql = f.read()
        setup_sql = setup_sql.replace(
            "https://trilogy-data.github.io/trilogy-public-models",
            str(Path(__file__).parent.parent),
        )
        engine.execute_raw_sql(setup_sql)
    if not found_setup:
        return None
    for file in file_path.glob("*.preql"):
        if file.name == "entrypoint.preql":
            continue
        print(f"Processing {file}")
        engine.environment = Environment(working_path=file.parent)
        engine.parse_file(file)

        try:
            validate_environment(engine.environment, exec=engine)
            validate_and_rewrite(file, engine)
            print("No validation errors found")
        except Exception as e:
            raise e
            print(f"Failed to process {file} with error: {e}")


if __name__ == "__main__":
    for file_path in DUCKDB_MODELS_PATH.iterdir():
        process_model(file_path)
