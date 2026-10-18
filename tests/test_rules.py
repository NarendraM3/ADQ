from types import SimpleNamespace
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
    assert "email is not a valid email address" in failed[0]["failure_reasons"]
    assert "age is below minimum 0" in failed[0]["failure_reasons"]
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
