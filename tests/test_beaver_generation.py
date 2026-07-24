import json

from scripts.beaver.collect_inferred_relationships import collect
from scripts.beaver.generate_models import render_table


def test_collect_requires_corroboration(tmp_path):
    ingest_root = tmp_path / "ingest"
    model_dir = ingest_root / "dw_full"
    model_dir.mkdir(parents=True)
    (model_dir / "facts.preql").write_text(
        "import dimensions as dimensions;\n"
        "root datasource facts (\n"
        "  DIMENSION_ID: ~dimensions.id,\n"
        "  OTHER_ID: dimensions.other_id,\n"
        ")\n",
        encoding="utf-8",
    )
    joins = tmp_path / "joins.json"
    joins.write_text(
        json.dumps(
            {
                "dw": [["FACTS.DIMENSION_ID", "DIMENSIONS.ID"]],
                "neutron": [],
                "nova": [],
            }
        ),
        encoding="utf-8",
    )
    ddl = tmp_path / "ddl.json"
    ddl.write_text(
        json.dumps({"foreign_keys": {"dw": [], "neutron": [], "nova": []}}),
        encoding="utf-8",
    )

    relationships = collect(ingest_root, joins, ddl)["dw"]

    assert relationships[0]["accepted"] is True
    assert relationships[0]["coverage"] == "partial"
    assert relationships[0]["corroborated_by"] == ["annotation"]
    assert relationships[1]["accepted"] is False


def test_render_table_uses_accepted_partial_inference_as_weak_import():
    table = {
        "table_name": "FACTS",
        "column_names": ["ID", "DIMENSION_ID"],
        "column_types": ["INT", "INT"],
    }
    inferred = {
        "dw": [
            {
                "source_table": "facts",
                "source_column": "dimension_id",
                "target_table": "dimensions",
                "target_column": "id",
                "coverage": "partial",
                "accepted": True,
            }
        ]
    }

    rendered = render_table("dw", table, {}, {}, inferred)

    assert "import dimensions as dimensions;" in rendered
    assert "`DIMENSION_ID`:~dimensions.col_id," in rendered
