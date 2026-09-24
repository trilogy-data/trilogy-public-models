#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = ["pytrilogy", "duckdb", "duckdb-engine"]
# ///
"""Regression guard: two `.count` shorthand metrics in one SELECT.

    SELECT supplier_id.count, nation_id.count;

Selecting the `<key>.count` shorthand for two different keys used to plan each
count as its own branch and then merge the branches with a keyless join.

    pytrilogy <= 0.3.315   correct                       (6, 3)
    pytrilogy 0.3.316-329  SILENTLY WRONG: cross join    (18, 18)
    pytrilogy 0.3.330-366  raises UnresolvableQueryException
                           ("Planner emitted a keyless join ... planner bug")
    pytrilogy >= 0.3.368   FIXED: correct again          (6, 3)

The exception in 0.3.330-366 was a guard correctly refusing SQL the planner had
been emitting since 0.3.316; the regression itself was 0.3.315 -> 0.3.316, and
the fix landed in 0.3.368 (0.3.367 was never published).

Writing the same counts inline -- `count(supplier_id), count(nation_id)` -- is
planned correctly on every version, so it serves as the oracle below. Only the
shorthand form, and only with two or more of them in one select, is affected:
`x.count` alone, or `x.count` next to an inline aggregate, is fine.

This is the shape behind two tpc_h dashboard queries in trilogy-public-models
(examples/duckdb/tpc_h/demo_dashboard.json, grid items 0 and 10), which were
skipped in tests/test_examples.py while the bug was live and now run again:

    SELECT part.supplier.id.count, part.supplier.nation.id.count;
    select order.id.count, order.customer.nation.id.count, part.supplier.nation.id.count;

Run against a specific release:

    uv run --with pytrilogy==0.3.315 repro_two_count_shorthand.py   # passes
    uv run --with pytrilogy==0.3.329 repro_two_count_shorthand.py   # wrong rows
    uv run --with pytrilogy==0.3.365 repro_two_count_shorthand.py   # raises
    uv run --with pytrilogy==0.3.368 repro_two_count_shorthand.py   # passes

Also collectable by pytest (`pytest repro_two_count_shorthand.py`).
"""
from __future__ import annotations

import sys

import trilogy
from trilogy import Dialects, Environment
from trilogy.executor import Executor

# Case 1 -- one table. `nation_id` is a foreign key carried on `supplier`, so
# both counts are answerable from that single datasource.
SINGLE_TABLE_MODEL = """
key nation_id int;
key supplier_id int;

datasource supplier (
    s_suppkey: supplier_id,
    s_nationkey: nation_id,
)
grain (supplier_id)
address supplier;
"""
SINGLE_TABLE_DATA = """
create table supplier as
select * from (values (1, 10), (2, 10), (3, 10), (4, 20), (5, 20), (6, 30))
    t(s_suppkey, s_nationkey);
"""

# Case 2 -- two tables. The second key is reached through a real join
# (orders -> customer), which is the shape of tpc_h's
# `order.id.count, order.customer.nation.id.count`.
JOINED_MODEL = """
key nation_id int;
key customer_id int;
key order_id int;

datasource customer (
    c_custkey: customer_id,
    c_nationkey: nation_id,
)
grain (customer_id)
address customer;

datasource orders (
    o_orderkey: order_id,
    o_custkey: customer_id,
)
grain (order_id)
address orders;
"""
JOINED_DATA = """
create table customer as
select * from (values (1, 10), (2, 10), (3, 20)) t(c_custkey, c_nationkey);
create table orders as
select * from (values (100, 1), (101, 1), (102, 2), (103, 3), (104, 3))
    t(o_orderkey, o_custkey);
"""

CASES = [
    (
        "single table",
        SINGLE_TABLE_MODEL,
        SINGLE_TABLE_DATA,
        "SELECT supplier_id.count, nation_id.count;",
        "SELECT count(supplier_id) as a, count(nation_id) as b;",
    ),
    (
        "joined tables",
        JOINED_MODEL,
        JOINED_DATA,
        "SELECT order_id.count, nation_id.count;",
        "SELECT count(order_id) as a, count(nation_id) as b;",
    ),
]


def build(model: str, data: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    for statement in filter(None, (s.strip() for s in data.split(";"))):
        executor.execute_raw_sql(statement)
    executor.parse_text(model)
    return executor


def rows(executor: Executor, query: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]


def check(name: str, model: str, data: str, shorthand: str, inline: str) -> str | None:
    """Return a description of the failure, or None if the shorthand is right."""
    expected = rows(build(model, data), inline)
    executor = build(model, data)
    try:
        actual = rows(executor, shorthand)
    except Exception as exc:  # noqa: BLE001 - report whatever the planner raises
        return f"{name}: `{shorthand}` raised {type(exc).__name__}: {exc}"
    if actual != expected:
        sql = executor.generate_sql(shorthand)[-1]
        return (
            f"{name}: `{shorthand}` returned {actual}, but the inline form "
            f"`{inline}` returns {expected}.\nGenerated SQL:\n{sql}"
        )
    return None


def test_two_count_shorthand_single_table():
    assert (failure := check(*CASES[0])) is None, failure


def test_two_count_shorthand_joined_tables():
    assert (failure := check(*CASES[1])) is None, failure


if __name__ == "__main__":
    print(f"pytrilogy {trilogy.__version__}")
    failures = [f for f in (check(*case) for case in CASES) if f]
    for failure in failures:
        print(f"\nFAIL {failure}")
    if not failures:
        print("OK: both shorthand queries match their inline-count oracle")
    sys.exit(1 if failures else 0)
