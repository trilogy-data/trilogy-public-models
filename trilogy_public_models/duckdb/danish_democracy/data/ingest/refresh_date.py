# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=17"]
# ///
import sys
from datetime import UTC, datetime

import pyarrow as pa

if __name__ == "__main__":
    table = pa.table(
        {"snapshot_date": pa.array([datetime.now(UTC).date()], type=pa.date32())}
    )
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)
