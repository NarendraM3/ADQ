"""Conversion of connector and dataframe values to strict JSON values."""

from datetime import date, datetime, time
from decimal import Decimal
import math
from collections.abc import Mapping


def json_safe(value):
    """Recursively convert common tabular values to JSON-compatible values.

    Missing dataframe/scalar values become ``None``. Unknown objects raise a
    TypeError so callers retain a visible serialization failure and traceback.
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    module = type(value).__module__.split(".", 1)[0]
    name = type(value).__name__
    if module == "pandas" and name in {"NAType", "NaTType"}:
        return None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return int(value) if value == value.to_integral_value() else float(value)

    # Pandas NA/NaT and NumPy scalar values are optional connector outputs.
    # Prefer their public scalar conversion methods without importing either
    # dependency on the normal local execution path.
    if module == "numpy":
        if hasattr(value, "tolist"):
            return json_safe(value.tolist())
        if name == "datetime64":
            return None if str(value) == "NaT" else str(value)
        if name == "timedelta64":
            return str(value)
        if hasattr(value, "item"):
            return json_safe(value.item())

    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [json_safe(item) for item in value]

    # Pandas Series/Index and record-like objects expose one of these methods.
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return json_safe(to_dict())
    as_dict = getattr(value, "_asdict", None)
    if callable(as_dict):
        return json_safe(as_dict())
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
