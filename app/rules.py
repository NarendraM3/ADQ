import re
from datetime import datetime

RULE_TYPES = {"NOT_NULL", "UNIQUE", "DATA_TYPE", "RANGE", "EMAIL_FORMAT", "DUPLICATES", "REGEX", "LENGTH", "IN_SET"}
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def validate_rule_definition(kind: str, params: dict):
    if kind not in RULE_TYPES: raise ValueError("Unsupported rule type")
    if kind == "DATA_TYPE" and str(params.get("type", "string")).lower() not in {"string","str","int","integer","float","number","decimal","bool","boolean","date","datetime"}:
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


def validate_records(records: list[dict], rules: list) -> tuple[list, list]:
    failures = {}
    for rule in rules:
        if not rule.enabled or not rule.approved:
            continue
        kind, col, params = rule.rule_type.upper(), rule.column_name, rule.parameters or {}
        seen = set()
        for idx, row in enumerate(records):
            value = row.get(col)
            reasons = failures.setdefault(idx, [])
            empty = value is None or str(value).strip() == ""
            reason = None
            if kind == "NOT_NULL" and empty: reason = f"{col} is required"
            elif kind in ("UNIQUE", "DUPLICATES") and not empty:
                key = str(value)
                if key in seen: reason = f"{col} duplicates value {key!r}"
                seen.add(key)
            elif kind == "EMAIL_FORMAT" and not empty and not EMAIL_RE.fullmatch(str(value)): reason = f"{col} is not a valid email address"
            elif kind == "DATA_TYPE" and not empty:
                typ = str(params.get("type", "string")).lower()
                try:
                    if typ in ("int", "integer"): int(value)
                    elif typ in ("float", "number", "decimal"): float(value)
                    elif typ in ("bool", "boolean") and str(value).lower() not in ("true", "false", "0", "1"): raise ValueError()
                    elif typ in ("date", "datetime"): datetime.fromisoformat(str(value))
                except (ValueError, TypeError): reason = f"{col} cannot be parsed as {typ}"
            elif kind == "RANGE" and not empty:
                try:
                    num = float(value)
                    if params.get("min") is not None and num < float(params["min"]): reason = f"{col} is below minimum {params['min']}"
                    if params.get("max") is not None and num > float(params["max"]): reason = f"{col} is above maximum {params['max']}"
                except (ValueError, TypeError): reason = f"{col} must be numeric for RANGE"
            elif kind == "REGEX" and not empty and params.get("pattern") and not re.fullmatch(params["pattern"], str(value)): reason = f"{col} does not match required pattern"
            elif kind == "LENGTH" and not empty:
                n = len(str(value))
                if params.get("min") is not None and n < int(params["min"]): reason = f"{col} is shorter than {params['min']}"
                if params.get("max") is not None and n > int(params["max"]): reason = f"{col} is longer than {params['max']}"
            elif kind == "IN_SET" and not empty and str(value) not in [str(x) for x in params.get("values", [])]: reason = f"{col} is not in allowed values"
            if reason: reasons.append(reason)
        
    passed, failed = [], []
    for i, record in enumerate(records):
        reasons = failures.get(i, [])
        if reasons:
            failed.append({"row_index": i, "record": record, "failure_reasons": reasons})
        else:
            passed.append(record)
    return passed, failed
