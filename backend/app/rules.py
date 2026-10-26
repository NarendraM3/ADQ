import json
import math
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

RULE_TYPES = {"NOT_NULL", "UNIQUE", "DATA_TYPE", "RANGE", "EMAIL_FORMAT", "DUPLICATES", "REGEX", "LENGTH", "IN_SET"}
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def is_missing(value) -> bool:
    """Recognize database, Python, and common dataframe missing values safely."""
    if value is None:
        return True
    if type(value).__name__ in {"NAType", "NaTType"}:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (float, Decimal)):
        try:
            return math.isnan(float(value))
        except (ValueError, TypeError, OverflowError):
            return False
    # Handles scalar NA objects without importing pandas or invoking ambiguous
    # array truth values.
    try:
        unequal = value != value
        return isinstance(unequal, bool) and unequal
    except (TypeError, ValueError):
        return False


def rule_signature(rule):
    params = {k: v for k, v in (rule.parameters or {}).items() if k not in {"severity", "rationale", "baseline_id"}}
    return (getattr(rule, "dataset_id", None), rule.column_name, rule.rule_type.upper(),
            json.dumps(params, sort_keys=True, separators=(",", ":"), default=str))


def resolve_column(record: dict, requested: str) -> str:
    """Resolve a rule column by exact spelling, then case/outer-space only."""
    if requested in record:
        return requested
    key = requested.strip().casefold()
    matches = [name for name in record if isinstance(name, str) and name.strip().casefold() == key]
    if len(matches) > 1:
        raise ValueError(f"Ambiguous column {requested!r}: multiple source columns normalize to the same name")
    if not matches:
        raise ValueError(f"Rule column {requested!r} does not exist in the executed dataset")
    return matches[0]


def validate_rule_definition(kind: str, params: dict):
    if kind not in RULE_TYPES: raise ValueError("Unsupported rule type")
    if kind == "DATA_TYPE" and str(params.get("type", "string")).lower() not in {"string", "str", "int", "integer", "float", "number", "decimal", "bool", "boolean", "date", "datetime"}:
        raise ValueError("DATA_TYPE type must be string, integer, number, boolean, date, or datetime")
    if kind in ("RANGE", "LENGTH"):
        for bound in ("min", "max"):
            if params.get(bound) is not None:
                try: float(params[bound]) if kind == "RANGE" else int(params[bound])
                except (ValueError, TypeError): raise ValueError(f"{bound} must be numeric")
        cast = float if kind == "RANGE" else int
        if params.get("min") is not None and params.get("max") is not None and cast(params["min"]) > cast(params["max"]):
            raise ValueError("min must be less than or equal to max")
    if kind == "REGEX":
        if not isinstance(params.get("pattern"), str): raise ValueError("REGEX requires a pattern")
        try: re.compile(params["pattern"])
        except re.error as e: raise ValueError(f"Invalid regex pattern: {e}") from e
    if kind == "IN_SET" and not isinstance(params.get("values"), list): raise ValueError("IN_SET requires a values array")


def _number(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite number")
    return number


def validate_records(records: list[dict], rules: list) -> tuple[list, list]:
    """Return passing records and one failure entry per failing input record.

    A record passes only when every distinct approved, enabled rule passes.
    Duplicate rules use a deterministic first-occurrence policy: the first ID
    passes and each subsequent occurrence fails, so a duplicate pair counts as
    one failed record while still identifying the duplicate value and row.
    """
    failures = {}
    executed = set()
    for rule in rules:
        if not rule.enabled or not rule.approved:
            continue
        kind, col, params = rule.rule_type.upper(), rule.column_name, rule.parameters or {}
        signature = rule_signature(rule)
        if signature in executed:
            continue
        executed.add(signature)
        compiled_pattern = re.compile(params["pattern"]) if kind == "REGEX" and params.get("pattern") else None
        accepted_values = None
        if kind == "IN_SET":
            accepted_values = {str(item).strip() for item in params.get("values", [])}
            if params.get("case_sensitive", True) is False:
                accepted_values = {item.casefold() for item in accepted_values}
        # Resolve once against the actual execution records. Missing or ambiguous
        # columns are configuration errors, never row-level null values.
        resolved = resolve_column(records[0], col) if records else None
        seen = set()
        for idx, row in enumerate(records):
            actual_col = resolved if resolved in row else resolve_column(row, col)
            value = row.get(actual_col)
            if is_missing(value):
                if kind in {"NOT_NULL", "EMAIL_FORMAT"}:
                    reason = f"Missing required {col}: the {col} field is empty."
                    reasons = failures.setdefault(idx, [])
                    if reason not in reasons:
                        reasons.append(reason)
                # Missing values are exclusively a required-field failure for
                # numeric, set, email and duplicate checks.
                continue
            shown = repr(value)
            reason = None
            text = str(value).strip()
            if kind in ("UNIQUE", "DUPLICATES"):
                key = text
                if key in seen: reason = f"{col} duplicates value {key!r}"
                seen.add(key)
            elif kind == "EMAIL_FORMAT" and not EMAIL_RE.fullmatch(text):
                reason = f"Invalid {col} format: {shown} is not a valid email address."
            elif kind == "DATA_TYPE":
                typ = str(params.get("type", params.get("expected_type", "string"))).lower()
                try:
                    if typ in ("int", "integer"):
                        n = _number(value)
                        if not n.is_integer(): raise ValueError()
                    elif typ in ("float", "number", "decimal"):
                        _number(value)
                    elif typ in ("bool", "boolean") and text.lower() not in ("true", "false", "0", "1"):
                        raise ValueError()
                    elif typ in ("date", "datetime"):
                        datetime.fromisoformat(text)
                except (ValueError, TypeError, OverflowError, InvalidOperation):
                    reason = f"Invalid {col}: {shown} cannot be parsed as {typ}."
            elif kind == "RANGE":
                try:
                    num = _number(value)
                    if params.get("min") is not None and num < float(params["min"]): reason = f"Invalid {col}: {shown} is below the allowed minimum of {params['min']}."
                    elif params.get("max") is not None and num > float(params["max"]): reason = f"Invalid {col}: {shown} is above the allowed maximum of {params['max']}."
                except (ValueError, TypeError, OverflowError):
                    reason = f"Invalid {col}: {shown} must be numeric for the configured range."
            elif kind == "REGEX" and compiled_pattern and not compiled_pattern.fullmatch(text):
                reason = f"{col} does not match required pattern"
            elif kind == "LENGTH":
                n = len(text)
                if params.get("min") is not None and n < int(params["min"]): reason = f"{col} is shorter than {params['min']}"
                elif params.get("max") is not None and n > int(params["max"]): reason = f"{col} is longer than {params['max']}"
            elif kind == "IN_SET":
                candidate = text
                if params.get("case_sensitive", True) is False:
                    candidate = candidate.casefold()
                if candidate not in accepted_values: reason = f"{col} value {shown} is not in the allowed values."
            if reason:
                reasons = failures.setdefault(idx, [])
                if reason not in reasons:
                    reasons.append(reason)

    passed, failed = [], []
    for i, record in enumerate(records):
        reasons = failures.get(i, [])
        if reasons:
            failed.append({"row_index": i, "record": record, "failure_reasons": reasons})
        else:
            passed.append(record)
    return passed, failed
