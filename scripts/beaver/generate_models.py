"""Generate Trilogy schema models from BEAVER metadata and its MySQL dump."""

from __future__ import annotations

import argparse
import json
import re
import zipfile
from collections import defaultdict
from pathlib import Path

DATABASES = ("dw", "neutron", "nova")
CREATE_START_RE = re.compile(r"CREATE TABLE `(?P<table>[^`]+)` \(", re.IGNORECASE)
PRIMARY_RE = re.compile(r"PRIMARY KEY \((?P<columns>[^)]+)\)", re.IGNORECASE)
FOREIGN_RE = re.compile(
    r"FOREIGN KEY \((?P<local>[^)]+)\) REFERENCES "
    r"`(?P<table>[^`]+)` \((?P<remote>[^)]+)\)",
    re.IGNORECASE,
)


DOMAIN_RULES: dict[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "neutron": (
        (
            "security",
            ("security", "default_security", "firewall", "ipsec", "ike", "vpn"),
        ),
        ("qos", ("qos",)),
        (
            "vendor",
            ("cisco", "arista", "brocade", "ml2", "nsx", "nuage", "vcns", "tz_"),
        ),
        ("operations", ("agent", "networkdhcpagent", "networkrbac")),
        ("system", ("alembic", "migrate", "quota", "reservation", "shadow_", "tag")),
        (
            "core",
            (
                "network",
                "port",
                "subnet",
                "router",
                "floatingip",
                "ipallocation",
                "ipavailability",
                "ipam",
                "dns",
                "externalnetwork",
            ),
        ),
        ("load_balancing", ("lbaas",)),
    ),
    "nova": (
        ("security", ("security", "key_pair")),
        (
            "compute",
            (
                "instance",
                "aggregate",
                "compute",
                "migration",
                "pci",
                "allocation",
                "inventory",
            ),
        ),
        ("operations", ("service", "console", "block_device", "virtual_interface")),
        (
            "system",
            (
                "alembic",
                "migrate",
                "quota",
                "reservation",
                "shadow_",
                "tag",
                "task_log",
            ),
        ),
    ),
    "dw": (
        ("facilities", ("fac_", "fclt_", "space", "building")),
        (
            "academics",
            ("academic", "course", "cis_", "sis_", "tip_", "library", "iap_"),
        ),
        ("people", ("hr_", "employee", "person", "opa_", "se_person", "master_dept")),
        ("system", ("alembic", "migrate", "shadow_", "etl_", "stg_")),
    ),
}

DOMAIN_OVERRIDES: dict[str, dict[str, str]] = {
    "neutron": {
        "healthmonitors": "load_balancing",
        "members": "load_balancing",
        "poolloadbalanceragentbindings": "load_balancing",
        "poolmonitorassociations": "load_balancing",
        "pools": "load_balancing",
        "poolstatisticss": "load_balancing",
        "providerresourceassociations": "load_balancing",
        # ML2 is Neutron's primary modular layer, and its port binding facts are
        # required alongside ports/IP allocations in common operational queries.
        # Keeping this high-value table under core avoids confusion with the
        # legacy PORTBINDINGPORTS extension.
        "ml2_port_bindings": "core",
        "sessionpersistences": "load_balancing",
        "subnetpoolprefixes": "core",
        "subnetpools": "core",
        "vips": "load_balancing",
    }
}

MODEL_DESCRIPTIONS: dict[tuple[str, str], str] = {
    (
        "neutron",
        "ipallocationpools",
    ): "Subnet allocation-pool definitions; FIRST_IP/LAST_IP are pool bounds.",
    (
        "neutron",
        "ipavailabilityranges",
    ): "Currently available address ranges within an IP allocation pool.",
    (
        "neutron",
        "lbaas_loadbalancer_statistics",
    ): (
        "Per-load-balancer traffic statistics. Join col_loadbalancer_id to a "
        "load-balancer col_id; statistic fields are local/root concepts."
    ),
    (
        "neutron",
        "ml2_port_bindings",
    ): (
        "Primary ML2 port-binding facts, including HOST and VIF_TYPE. Use this "
        "for host/VIF questions; PORTBINDINGPORTS is a legacy host-only table."
    ),
    (
        "neutron",
        "portbindingports",
    ): (
        "Legacy host-only port binding extension. It does not contain VIF_TYPE; "
        "prefer ML2_PORT_BINDINGS for host/VIF questions."
    ),
    (
        "neutron",
        "providerresourceassociations",
    ): (
        "Provider assignments for polymorphic resources. The weak "
        "lbaas_loadbalancers role exists only where RESOURCE_ID is a load balancer."
    ),
    (
        "neutron",
        "subnetroutes",
    ): "Routes keyed by destination/next-hop and attached to a subnet.",
}

# A one-to-one extension whose PK is also an FK normally attaches every local
# property to the imported key. PyTrilogy cannot currently preserve that local
# datasource binding through a second import, so keep these extension facts at
# a local grain and expose their join key explicitly.
LOCAL_FOREIGN_KEYS: set[tuple[str, str, str]] = {
    ("neutron", "lbaas_loadbalancer_statistics", "loadbalancer_id"),
}


def quoted_columns(value: str) -> tuple[str, ...]:
    return tuple(re.findall(r"`([^`]+)`", value))


def read_dump_metadata(
    archive: Path,
) -> tuple[
    dict[str, dict[str, tuple[str, ...]]],
    dict[str, list[tuple[str, str, str, str]]],
]:
    primary_keys: dict[str, dict[str, tuple[str, ...]]] = defaultdict(dict)
    foreign_keys: dict[str, list[tuple[str, str, str, str]]] = defaultdict(list)
    with zipfile.ZipFile(archive) as bundle:
        for db in DATABASES:
            table: str | None = None
            body_lines: list[str] = []
            for raw_line in bundle.open(f"beaver_db/{db}.sql"):
                line = raw_line.decode("utf-8", "replace")
                if table is None:
                    start = CREATE_START_RE.search(line)
                    if start:
                        table = start.group("table")
                        body_lines = []
                    continue
                if line.startswith(") ENGINE="):
                    body = "".join(body_lines)
                else:
                    body_lines.append(line)
                    continue
                primary = PRIMARY_RE.search(body)
                if primary:
                    primary_keys[db][table.lower()] = quoted_columns(
                        primary.group("columns")
                    )
                for foreign in FOREIGN_RE.finditer(body):
                    local = quoted_columns(foreign.group("local"))
                    remote = quoted_columns(foreign.group("remote"))
                    if len(local) == len(remote):
                        foreign_keys[db].extend(
                            (
                                table,
                                local_column,
                                foreign.group("table"),
                                remote_column,
                            )
                            for local_column, remote_column in zip(local, remote)
                        )
                table = None
                body_lines = []
    return dict(primary_keys), dict(foreign_keys)


def trilogy_type(mysql_type: str) -> str:
    normalized = mysql_type.upper()
    if any(token in normalized for token in ("INT", "NUMBER")):
        return "int"
    if any(
        token in normalized
        for token in ("DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL")
    ):
        return "float"
    if "BOOL" in normalized:
        return "bool"
    if "TIMESTAMP" in normalized or "DATETIME" in normalized:
        return "datetime"
    if normalized == "DATE":
        return "date"
    return "string"


def concept_name(column: str) -> str:
    # Prefixing preserves the physical column in the datasource mapping while
    # avoiding collisions with Trilogy keywords such as LIMIT and ORDER.
    return "col_" + column.lower()


def table_name_for_mysql(db: str, metadata_name: str) -> str:
    return metadata_name if db == "dw" else metadata_name.lower()


def property_declaration(parents: tuple[str, ...], column: str, datatype: str) -> str:
    if len(parents) == 1:
        return f"property {parents[0]}.{column} {datatype};"
    return f"property <{', '.join(parents)}>.{column} {datatype};"


def property_reference(parents: tuple[str, ...], column: str) -> str:
    """Return the address created by ``property_declaration``.

    A property parented by one imported key is created in that key's namespace,
    not at the extension module root. PyTrilogy 0.3.301 validates this mapping
    eagerly, so the datasource must bind the qualified concept.
    """
    if len(parents) == 1 and "." in parents[0]:
        namespace, _ = parents[0].rsplit(".", 1)
        return f"{namespace}.{column}"
    return column


def render_table(
    db: str,
    table: dict,
    primary_keys: dict[str, dict[str, tuple[str, ...]]],
    foreign_keys: dict[str, list[tuple[str, str, str, str]]],
    inferred_relationships: dict[str, list[dict[str, object]]],
) -> str:
    metadata_name = table["table_name"]
    mysql_name = table_name_for_mysql(db, metadata_name)
    table_key = mysql_name.lower()
    columns = [concept_name(value) for value in table["column_names"]]
    types = [trilogy_type(value) for value in table["column_types"]]
    table_foreign_keys = [
        (*item, False)
        for item in foreign_keys.get(db, [])
        if item[0].lower() == table_key
        and (db, table_key, item[1].lower()) not in LOCAL_FOREIGN_KEYS
    ]
    declared_columns = {item[1].lower() for item in table_foreign_keys}
    table_foreign_keys.extend(
        (
            str(item["source_table"]),
            str(item["source_column"]),
            str(item["target_table"]),
            str(item["target_column"]),
            item.get("coverage") == "partial",
        )
        for item in inferred_relationships.get(db, [])
        if item.get("accepted")
        and str(item["source_table"]).lower() == table_key
        and str(item["source_column"]).lower() not in declared_columns
    )
    remote_counts: dict[str, int] = defaultdict(int)
    for _, _, remote_table, _, _ in table_foreign_keys:
        remote_counts[remote_table.lower()] += 1

    foreign_concepts: dict[str, tuple[str, bool]] = {}
    imports: dict[str, str] = {}
    for _, local_column, remote_table, remote_column, weak in table_foreign_keys:
        local_key = local_column.lower()
        remote_key = remote_table.lower()
        if remote_key == table_key:
            continue
        remote_module = module_name(remote_table)
        alias = (
            remote_module
            if remote_counts[remote_key] == 1
            else f"{concept_name(local_column)}_{remote_module}"
        )
        imports[alias] = relative_module_path(
            domain_for_table(db, metadata_name),
            domain_for_table(db, remote_table),
            remote_module,
        )
        foreign_concepts[local_key] = (
            f"{alias}.{concept_name(remote_column)}",
            weak,
        )

    primary = tuple(
        foreign_concepts.get(value.lower(), (concept_name(value), False))[0]
        for value in primary_keys.get(db, {}).get(table_key, ())
    )
    if not primary:
        # The anonymized DW dump omits most PK constraints. A full-row grain is
        # conservative: it does not invent uniqueness for one arbitrary column.
        primary = tuple(
            foreign_concepts.get(original.lower(), (column, False))[0]
            for original, column in zip(table["column_names"], columns)
        )

    description = MODEL_DESCRIPTIONS.get((db, table_key))
    lines = [f"# {description}", ""] if description else []
    lines.extend(
        f"import {remote_module} as {alias};"
        for alias, remote_module in sorted(imports.items())
    )
    if imports:
        lines.append("")
    for original, column, datatype in zip(table["column_names"], columns, types):
        resolved = foreign_concepts.get(original.lower(), (column, False))[0]
        if original.lower() in foreign_concepts:
            continue
        if resolved in primary:
            lines.append(f"key {column} {datatype};")
        else:
            lines.append(property_declaration(primary, column, datatype))

    lines.extend(("", "datasource source ("))
    for original, column in zip(table["column_names"], columns):
        resolved, weak = foreign_concepts.get(original.lower(), (column, False))
        if original.lower() not in foreign_concepts and resolved not in primary:
            resolved = property_reference(primary, column)
        if weak:
            resolved = f"~{resolved}"
        lines.append(f"    `{original}`:{resolved},")
    lines.extend(
        (
            ")",
            f"grain ({', '.join(primary)})",
            f"address `{db}.{mysql_name}`;",
            "",
        )
    )
    return "\n".join(lines)


def module_name(table_name: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", table_name.lower()).strip("_")


def domain_for_table(db: str, table_name: str) -> str:
    normalized = module_name(table_name)
    override = DOMAIN_OVERRIDES.get(db, {}).get(normalized)
    if override:
        return override
    for domain, tokens in DOMAIN_RULES.get(db, ()):
        if any(token in normalized for token in tokens):
            return domain
    return "other"


def relative_module_path(
    source_domain: str, target_domain: str, target_module: str
) -> str:
    if source_domain == target_domain:
        return target_module
    return f"..{target_domain}.{target_module}"


def render_entrypoint(db: str) -> str:
    return (
        f"# BEAVER {db} discovery root.\n"
        "# Table models are grouped into domain folders and intentionally are not\n"
        "# imported here. Import the relevant table module directly.\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", type=Path, default=Path("data/beaver/schema.json"))
    parser.add_argument(
        "--joins", type=Path, default=Path("data/beaver/join_keys.json")
    )
    parser.add_argument(
        "--inferred",
        type=Path,
        default=Path("data/beaver/inferred_relationships.json"),
    )
    parser.add_argument(
        "--curated",
        type=Path,
        default=Path("data/beaver/curated_relationships.json"),
    )
    parser.add_argument("--ddl-zip", type=Path)
    parser.add_argument(
        "--ddl-metadata",
        type=Path,
        default=Path("data/beaver/ddl_metadata.json"),
    )
    parser.add_argument(
        "--output", type=Path, default=Path("trilogy_public_models/mysql")
    )
    args = parser.parse_args()

    schema = json.loads(args.schema.read_text(encoding="utf-8"))
    inferred_relationships = (
        json.loads(args.inferred.read_text(encoding="utf-8"))
        if args.inferred.exists()
        else {}
    )
    if args.curated.exists():
        curated_relationships = json.loads(args.curated.read_text(encoding="utf-8"))
        for db, relationships in curated_relationships.items():
            inferred_relationships.setdefault(db, []).extend(relationships)
    primary_keys: dict[str, dict[str, tuple[str, ...]]] = {}
    foreign_keys: dict[str, list[tuple[str, str, str, str]]] = {}
    if args.ddl_zip:
        primary_keys, foreign_keys = read_dump_metadata(args.ddl_zip)
    elif args.ddl_metadata.exists():
        ddl_metadata = json.loads(args.ddl_metadata.read_text(encoding="utf-8"))
        primary_keys = {
            db: {table: tuple(columns) for table, columns in tables.items()}
            for db, tables in ddl_metadata.get("primary_keys", {}).items()
        }
        foreign_keys = {
            db: [tuple(item) for item in items]
            for db, items in ddl_metadata.get("foreign_keys", {}).items()
        }

    for db in DATABASES:
        model_dir = args.output / f"beaver_{db}"
        model_dir.mkdir(parents=True, exist_ok=True)
        tables = list(schema[db].values())
        for stale in model_dir.rglob("*.preql"):
            stale.unlink()
        for table in tables:
            path = (
                model_dir
                / domain_for_table(db, table["table_name"])
                / f"{module_name(table['table_name'])}.preql"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                render_table(
                    db,
                    table,
                    primary_keys,
                    foreign_keys,
                    inferred_relationships,
                ),
                encoding="utf-8",
                newline="\n",
            )
        (model_dir / "entrypoint.preql").write_text(
            render_entrypoint(db),
            encoding="utf-8",
            newline="\n",
        )
        (model_dir / "README.md").write_text(
            f"# BEAVER {db}\n\n"
            f"Generated semantic schema for the BEAVER `{db}` MySQL database.\n"
            "Table modules are grouped into domain folders. `entrypoint.preql` is "
            "a lightweight discovery root; import the relevant table module "
            "directly.\n\n"
            "Regenerate with `scripts/beaver/generate_models.py`; do not edit "
            "individual table files manually.\n",
            encoding="utf-8",
            newline="\n",
        )
        (model_dir / "trilogy.toml").write_text(
            '[engine]\ndialect = "mysql"\n',
            encoding="utf-8",
            newline="\n",
        )

    metadata = {
        "primary_keys": {
            db: {table: list(columns) for table, columns in sorted(tables.items())}
            for db, tables in sorted(primary_keys.items())
        },
        "foreign_keys": {
            db: [list(item) for item in sorted(items)]
            for db, items in sorted(foreign_keys.items())
        },
    }
    metadata_path = args.ddl_metadata
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
