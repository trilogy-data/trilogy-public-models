from trilogy.core.validation.fix import rewrite_file_with_errors
from pathlib import Path
from trilogy import Dialects

DUCKDB_MODELS_PATH = Path(__file__).parent / "trilogy_public_models" / "duckdb"


def process_model(file_path: Path):
    # find the setup file
    engine = Dialects.DUCK_DB.default_executor()
    for setup_file in file_path.glob("*.setup.sql"):
        with open(setup_file, "r") as f:
            setup_sql = f.read()
        engine.execute_raw_sql(setup_sql)

    for file in file_path.glob("*.preql"):
        print(f"Processing {file}")
        try:
            rewrite_file_with_errors(file, engine)
        except Exception as e:
            print(f"Failed to process {file} with error: {e}")


if __name__ == "__main__":
    for file_path in DUCKDB_MODELS_PATH.iterdir():
        process_model(file_path)