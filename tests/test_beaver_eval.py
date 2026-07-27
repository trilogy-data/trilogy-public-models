from scripts.beaver.audit_question_specification import (
    audit_question,
    sql_physical_tables,
)
from scripts.beaver.eval_neutron import (
    canonicalize_rows,
    load_questions,
    load_specification,
    wilson_interval,
)


def test_canonicalize_rows_matches_beaver_semantics():
    left = [(2, " value "), (1, None), (2, "value")]
    right = [(1, None), (2, "value")]

    assert canonicalize_rows(left) == canonicalize_rows(right)


def test_load_questions_filters_neutron(tmp_path):
    questions = tmp_path / "questions.jsonl"
    questions.write_text(
        '{"id":"dw_0","split":"dw"}\n' '{"id":"neutron_0","split":"neutron"}\n',
        encoding="utf-8",
    )

    assert [row["id"] for row in load_questions(questions)] == ["neutron_0"]


def test_audit_flags_a_gold_join_table_missing_from_declared_tables():
    audited = audit_question(
        {
            "id": "neutron_x",
            "split": "neutron",
            "tables": ["PORTS", "IPALLOCATIONS"],
            "join_keys": [
                ["PORTS.ID", "PORTSECURITYBINDINGS.PORT_ID"],
                ["PORTS.ID", "IPALLOCATIONS.PORT_ID"],
            ],
            "sql": (
                "WITH selected AS (SELECT p.id FROM ports p "
                "JOIN portsecuritybindings psb ON p.id = psb.port_id) "
                "SELECT * FROM selected"
            ),
        }
    )

    assert audited["status"] == "unspecified"
    assert audited["evaluation_tier"] == "excluded"
    assert audited["reasons"][0]["tables"] == ["PORTSECURITYBINDINGS"]


def test_load_specification(tmp_path):
    path = tmp_path / "specification.jsonl"
    path.write_text(
        '{"id":"neutron_0","status":"specified"}\n'
        '{"id":"neutron_1","status":"unspecified"}\n',
        encoding="utf-8",
    )

    assert load_specification(path)["neutron_1"]["status"] == "unspecified"


def test_audit_marks_two_table_question_without_domain_annotation_high_confidence():
    audited = audit_question(
        {
            "id": "neutron_x",
            "split": "neutron",
            "tables": ["PORTS", "ML2_PORT_BINDINGS"],
            "join_keys": [["PORTS.ID", "ML2_PORT_BINDINGS.PORT_ID"]],
            "contains_domain_knowledge": False,
            "sql": (
                "SELECT p.id FROM ports p JOIN ml2_port_bindings b "
                "ON p.id = b.port_id"
            ),
        }
    )

    assert audited["status"] == "specified"
    assert audited["evaluation_tier"] == "high_confidence"


def test_sql_table_extraction_ignores_words_inside_literals():
    assert sql_physical_tables(
        "SELECT 'Deviation From Average' AS label FROM ports"
    ) == {"PORTS"}


def test_wilson_interval_contains_observed_pass_rate():
    low, high = wilson_interval(7, 10)

    assert low < 0.7 < high
    assert (low, high) == wilson_interval(7, 10)
