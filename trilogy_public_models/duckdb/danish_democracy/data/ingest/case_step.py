# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=17"]
# ///
from source import emit  # type: ignore[import-not-found]

if __name__ == "__main__":
    emit("case_step")
