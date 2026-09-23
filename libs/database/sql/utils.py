from datetime import date, datetime
from typing import Any


def format_sql_value(val: Any) -> str:
    """Formats Python values or raw strings into safe SQL literals or unquoted expressions."""
    result: str

    match val:
        case None:
            result = "NULL"
        case bool():
            result = "TRUE" if val else "FALSE"
        case int() | float():
            result = str(val)
        case datetime() | date():
            result = f"'{val.isoformat()}'"
        case _:
            val_str = str(val).strip()
            upper_val = val_str.upper()

            # Numeric strings, SQL keywords, or pre-quoted strings pass through unquoted
            if (
                _is_numeric(val_str)
                or _is_sql_keyword(upper_val)
                or _is_quoted(val_str)
            ):
                result = (
                    val_str if upper_val not in ("TRUE", "FALSE", "NULL") else upper_val
                )
            else:
                escaped = val_str.replace("'", "''")
                result = f"'{escaped}'"

    return result


def _is_numeric(val: str) -> bool:
    try:
        float(val)
        return True
    except ValueError:
        return False


def _is_sql_keyword(val_upper: str) -> bool:
    return val_upper in (
        "TRUE",
        "FALSE",
        "NULL",
        "CURRENT_TIMESTAMP",
        "NOW()",
        "CURRENT_DATE",
    )


def _is_quoted(val: str) -> bool:
    return len(val) >= 2 and val.startswith("'") and val.endswith("'")
