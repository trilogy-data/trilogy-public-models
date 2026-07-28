import json

from scripts.beaver.collect_inferred_relationships import collect
from scripts.beaver.generate_models import (
    domain_for_table,
    relative_module_path,
    render_entrypoint,
    render_table,
)


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


def test_neutron_models_are_grouped_and_cross_domain_imports_are_relative():
    assert domain_for_table("neutron", "securitygroups") == "security"
    assert domain_for_table("neutron", "networks") == "core"
    assert relative_module_path("security", "core", "networks") == "..core.networks"
    assert domain_for_table("neutron", "ipallocationpools") == "core"
    assert domain_for_table("neutron", "ipavailabilityranges") == "core"
    assert domain_for_table("neutron", "ml2_port_bindings") == "core"
    assert domain_for_table("neutron", "subnetpools") == "core"
    assert domain_for_table("neutron", "nsxv_edge_pool_mappings") == "vendor"
    assert (
        domain_for_table("neutron", "providerresourceassociations") == "load_balancing"
    )


def test_entrypoint_is_a_lightweight_discovery_root():
    rendered = render_entrypoint("neutron")

    assert "import " not in rendered
    assert "Import the relevant table module directly." in rendered


def test_localized_extension_fact_keeps_statistics_at_root():
    table = {
        "table_name": "LBAAS_LOADBALANCER_STATISTICS",
        "column_names": ["LOADBALANCER_ID", "BYTES_IN"],
        "column_types": ["VARCHAR(36)", "BIGINT"],
    }
    foreign_keys = {
        "neutron": [
            (
                "lbaas_loadbalancer_statistics",
                "loadbalancer_id",
                "lbaas_loadbalancers",
                "id",
            )
        ]
    }

    rendered = render_table(
        "neutron",
        table,
        {"neutron": {"lbaas_loadbalancer_statistics": ("loadbalancer_id",)}},
        foreign_keys,
        {},
    )

    assert "import lbaas_loadbalancers" not in rendered
    assert "key col_loadbalancer_id string;" in rendered
    assert "property col_loadbalancer_id.col_bytes_in int;" in rendered


def test_extension_properties_bind_in_imported_key_namespace():
    table = {
        "table_name": "EXTERNALNETWORKS",
        "column_names": ["NETWORK_ID", "IS_DEFAULT"],
        "column_types": ["VARCHAR(36)", "INT"],
    }
    foreign_keys = {
        "neutron": [
            ("externalnetworks", "network_id", "networks", "id"),
        ]
    }

    rendered = render_table(
        "neutron",
        table,
        {"neutron": {"externalnetworks": ("network_id",)}},
        foreign_keys,
        {},
    )

    assert "property networks.col_id.col_is_default int;" in rendered
    assert "`IS_DEFAULT`:networks.col_is_default," in rendered
