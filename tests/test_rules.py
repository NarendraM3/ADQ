from types import SimpleNamespace
import math
from app.rules import validate_records, validate_rule_definition


def test_validation_reasons_for_multiple_rules():
    records = [{"email":"bad", "age":"-2"}, {"email":"a@example.com", "age":"21"}, {"email":"a@example.com", "age":"wrong"}]
    rules = [
        SimpleNamespace(enabled=True, approved=True, rule_type="EMAIL_FORMAT", column_name="email", parameters={}),
        SimpleNamespace(enabled=True, approved=True, rule_type="RANGE", column_name="age", parameters={"min":0,"max":120}),
        SimpleNamespace(enabled=True, approved=True, rule_type="UNIQUE", column_name="email", parameters={}),
    ]
    passed, failed = validate_records(records, rules)
    assert passed == [{"email":"a@example.com", "age":"21"}]
    assert "Invalid email format: 'bad' is not a valid email address." in failed[0]["failure_reasons"]
    assert "Invalid age: '-2' is below the allowed minimum of 0." in failed[0]["failure_reasons"]
    third = next(row for row in failed if row["row_index"] == 2)
    assert any("duplicates" in x for x in third["failure_reasons"])


def test_unapproved_rules_are_not_applied():
    rule = SimpleNamespace(enabled=True, approved=False, rule_type="NOT_NULL", column_name="name", parameters={})
    passed, failed = validate_records([{"name":""}], [rule])
    assert passed == [{"name":""}]
    assert failed == []


def test_invalid_rule_parameters_are_rejected():
    try:
        validate_rule_definition("REGEX", {"pattern":"["})
        assert False, "invalid regex should be rejected"
    except ValueError as exc:
        assert "Invalid regex" in str(exc)


def test_approved_rules_fail_each_invalid_record_once():
    records = [
        {"customer_id": "1", "email": "alice@example.com", "age": "30"},
        {"customer_id": "2", "email": "not-an-email", "age": "25"},
        {"customer_id": "3", "email": "charlie@example.com", "age": "-5"},
        {"customer_id": "4", "email": "", "age": "40"},
        {"customer_id": "5", "email": "eric@example.com", "age": "28"},
    ]
    rules = [
        SimpleNamespace(enabled=True, approved=True, rule_type="EMAIL_FORMAT", column_name="email", parameters={}),
        SimpleNamespace(enabled=True, approved=True, rule_type="DATA_TYPE", column_name="age", parameters={"type": "integer"}),
        SimpleNamespace(enabled=True, approved=True, rule_type="RANGE", column_name="age", parameters={"min": 0}),
    ]

    passed, failed = validate_records(records, rules)

    assert len(records) == 5
    assert len(passed) == 2
    assert len(failed) == 3
    assert {item["row_index"] for item in failed} == {1, 2, 3}
    assert len({item["row_index"] for item in failed}) == len(failed)
    assert any("not a valid email" in reason for reason in failed[0]["failure_reasons"])
    assert any("below the allowed minimum of 0" in reason for reason in failed[1]["failure_reasons"])
    assert any("required" in reason for reason in failed[2]["failure_reasons"])
    assert failed[1]["record"]["customer_id"] == "3"
    assert "Invalid age: '-5' is below the allowed minimum of 0." in failed[1]["failure_reasons"]


def test_generated_integer_type_alias_is_enforced():
    records = [{"age": "not-an-integer"}, {"age": "-5"}]
    rules = [SimpleNamespace(enabled=True, approved=True, rule_type="DATA_TYPE", column_name="age",
                             parameters={"expected_type": "integer"})]

    passed, failed = validate_records(records, rules)

    assert passed == [{"age": "-5"}]
    assert len(failed) == 1
    assert "cannot be parsed as integer" in failed[0]["failure_reasons"][0]


def test_duplicate_ids_and_invalid_types_fail_only_affected_rows():
    records = [{"id": "1", "age": "20"}, {"id": "1", "age": "old"}, {"id": "3", "age": "30"}]
    rules = [
        SimpleNamespace(enabled=True, approved=True, rule_type="UNIQUE", column_name="id", parameters={}),
        SimpleNamespace(enabled=True, approved=True, rule_type="DATA_TYPE", column_name="age", parameters={"type": "integer"}),
    ]

    passed, failed = validate_records(records, rules)

    assert passed == [records[0], records[2]]
    assert len(failed) == 1
    assert len(failed[0]["failure_reasons"]) == 2


def test_null_whitespace_type_and_range_boundary_semantics():
    records = [
        {"required": None, "count": "1", "age": "0"},
        {"required": "   ", "count": "two", "age": "-0.01"},
        {"required": "ok", "count": "3", "age": "120"},
        {"required": "ok", "count": "4", "age": "120.01"},
    ]
    rules = [
        SimpleNamespace(enabled=True, approved=True, rule_type="NOT_NULL", column_name="required", parameters={}),
        SimpleNamespace(enabled=True, approved=True, rule_type="DATA_TYPE", column_name="count", parameters={"type": "integer"}),
        SimpleNamespace(enabled=True, approved=True, rule_type="RANGE", column_name="age", parameters={"min": 0, "max": 120}),
    ]

    passed, failed = validate_records(records, rules)

    assert passed == [records[2]]
    assert [row["row_index"] for row in failed] == [0, 1, 3]
    assert any("required" in reason for reason in failed[0]["failure_reasons"])
    assert any("cannot be parsed as integer" in reason for reason in failed[1]["failure_reasons"])
    assert any("above the allowed maximum of 120" in reason for reason in failed[2]["failure_reasons"])


def test_exact_duplicate_rule_runs_once_but_distinct_rule_is_preserved():
    records = [{"age": "-1"}]
    rules = [
        SimpleNamespace(enabled=True, approved=True, rule_type="RANGE", column_name="age", parameters={"min": 0, "severity": "low"}),
        SimpleNamespace(enabled=True, approved=True, rule_type="RANGE", column_name="age", parameters={"min": 0, "severity": "high", "rationale": "same check"}),
        SimpleNamespace(enabled=True, approved=True, rule_type="RANGE", column_name="age", parameters={"min": 1}),
    ]

    passed, failed = validate_records(records, rules)

    assert passed == []
    assert len(failed) == 1
    assert failed[0]["failure_reasons"].count("Invalid age: '-1' is below the allowed minimum of 0.") == 1
    assert failed[0]["failure_reasons"].count("Invalid age: '-1' is below the allowed minimum of 1.") == 1


def make_rule(kind, column, parameters=None, enabled=True, approved=True):
    return SimpleNamespace(enabled=enabled, approved=approved, rule_type=kind,
                           column_name=column, parameters=parameters or {}, dataset_id=1)


def test_required_values_include_null_empty_whitespace_and_nan():
    values = [None, "", "   ", math.nan]
    rules = [make_rule("NOT_NULL", "value")]
    for value in values:
        passed, failed = validate_records([{"value": value}], rules)
        assert passed == []
        assert len(failed) == 1
        assert "Missing required value" in failed[0]["failure_reasons"][0]


def test_email_format_and_missing_email_have_one_clear_required_reason():
    records = [{"email": "aarav.sharma@example.com"}, {"email": "kavya.singh.example.com"}, {"email": "  "}]
    rules = [make_rule("NOT_NULL", "email"), make_rule("EMAIL_FORMAT", "email")]
    passed, failed = validate_records(records, rules)
    assert passed == [records[0]]
    assert [item["row_index"] for item in failed] == [1, 2]
    assert len(failed[1]["failure_reasons"]) == 1
    assert "Missing required email" in failed[1]["failure_reasons"][0]
    assert "Invalid email format" in failed[0]["failure_reasons"][0]


def test_numeric_rules_handle_missing_negative_nonnumeric_and_float_strings():
    records = [{"Age": None}, {"Age": "-4"}, {"Age": "bad"}, {"Age": "28.0"}, {"Age": 0.0}]
    rules = [make_rule("NOT_NULL", "Age"), make_rule("DATA_TYPE", "Age", {"type": "number"}),
             make_rule("RANGE", "Age", {"min": 0})]
    passed, failed = validate_records(records, rules)
    assert passed == records[3:]
    assert [item["row_index"] for item in failed] == [0, 1, 2]
    assert any("Missing required Age" in x for x in failed[0]["failure_reasons"])
    assert any("below the allowed minimum" in x for x in failed[1]["failure_reasons"])
    assert any("cannot be parsed" in x for x in failed[2]["failure_reasons"])


def test_salary_missing_negative_and_nonnumeric_values_fail_without_crashing():
    records = [{"Salary": ""}, {"Salary": "-1500"}, {"Salary": "unknown"}, {"Salary": 54000.0}]
    rules = [make_rule("NOT_NULL", "Salary"), make_rule("DATA_TYPE", "Salary", {"type": "number"}),
             make_rule("RANGE", "Salary", {"min": 0})]
    passed, failed = validate_records(records, rules)
    assert passed == [records[3]]
    assert len(failed) == 3


def test_department_allowed_values_trim_and_case_normalize():
    records = [{"Department": " IT "}, {"Department": "hr"}, {"Department": "UnknownDept"}, {"Department": "  "}]
    rules = [make_rule("NOT_NULL", "Department"), make_rule("IN_SET", "Department",
        {"values": ["IT", "HR", "Finance", "Engineering", "Operations", "Marketing"], "case_sensitive": False})]
    passed, failed = validate_records(records, rules)
    assert passed == records[:2]
    assert [r["row_index"] for r in failed] == [2, 3]


def test_duplicate_employee_ids_report_second_occurrence_only():
    records = [{"EmployeeID": "EMP014"}, {"EmployeeID": "EMP014"}]
    passed, failed = validate_records(records, [make_rule("UNIQUE", "EmployeeID")])
    assert passed == [records[0]]
    assert len(failed) == 1
    assert failed[0]["row_index"] == 1
    assert "EMP014" in failed[0]["failure_reasons"][0]


def test_pending_and_disabled_rules_are_never_executed():
    records = [{"value": ""}]
    rules = [make_rule("NOT_NULL", "value", enabled=True, approved=False),
             make_rule("NOT_NULL", "value", enabled=False, approved=True)]
    passed, failed = validate_records(records, rules)
    assert passed == records
    assert failed == []
