from io import BytesIO
from types import SimpleNamespace

import pytest
from openpyxl import Workbook

from app.processing import parse_data, schema_from_records, validate_rule_bindings


def test_excel_parser_drops_empty_unnamed_columns_without_losing_source_fields():
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["EmployeeID", "Email", None, None])
    sheet.append(["EMP001", "a@example.com", None, None])
    content = BytesIO()
    workbook.save(content)

    rows = parse_data("employees.xlsx", content.getvalue())

    assert rows == [{"EmployeeID": "EMP001", "Email": "a@example.com"}]


def test_excel_parser_keeps_populated_column_without_a_header():
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["EmployeeID", "", "Email"])
    sheet.append(["EMP001", "source value", "a@example.com"])
    content = BytesIO()
    workbook.save(content)

    rows = parse_data("employees.xlsx", content.getvalue())

    assert rows == [{"EmployeeID": "EMP001", "column_2": "source value", "Email": "a@example.com"}]


def test_schema_metadata_is_refreshed_from_actual_records():
    schema = schema_from_records([{"EmployeeID": "EMP001", "Email": "a@example.com"},
                                  {"EmployeeID": "EMP002", "Email": None}])
    assert list(schema["columns"]) == ["EmployeeID", "Email"]
    assert schema["row_count"] == 2
    assert schema["columns"]["Email"]["nullable"] is True


def test_rule_binding_rejects_mismatched_dataset_or_schema():
    rule = SimpleNamespace(id=8, dataset_id=2, column_name="Email")
    rows = {1: [{"emp_id": "EMP001", "email": "a@example.com"}],
            2: [{"EmployeeID": "EMP001", "Email": "a@example.com"}]}

    with pytest.raises(ValueError, match="not selected by this job"):
        validate_rule_bindings([rule], [1], rows, rows[1])
    rule.dataset_id = 1
    rule.column_name = "dob"
    with pytest.raises(ValueError, match="does not exist in the executed dataset"):
        validate_rule_bindings([rule], [1], rows, rows[1])
