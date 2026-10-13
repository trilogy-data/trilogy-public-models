# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=17", "boto3>=1.36"]
# ///
from source import emit  # type: ignore[import-not-found]

if __name__ == "__main__":
    emit("case")
