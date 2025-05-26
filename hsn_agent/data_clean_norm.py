import pandas as pd
import re
from typing import Any


# Fucntion to parse the duty values and spliting them into separate columns which describes the value and unit of the duty.
def parse_duty_value(x: Any):
    """
    - "Rs. 42 / kg" or "₹42/kg"    -> (42.0, "INR/kg")
    - "120/kg"                     -> (120.0, "INR/kg")
    - plain number "10"            -> (0.10, "percentage")
    - numeric 10 or 10.0           -> (0.10, "percentage")
    - NaN/empty                    -> (None, None)
    """
    if pd.isna(x):
        return None, None

    # Numeric → percentage
    if isinstance(x, (int, float)):
        return float(x) / 100.0, "percentage"

    s = str(x).strip()

    # 1) Currency-per-unit pattern
    m = re.match(r"(?:Rs\.?|₹)?\s*([\d\.]+)\s*/\s*(\w+)", s, flags=re.IGNORECASE)
    if m:
        value = float(m.group(1))
        unit = m.group(2)
        return value, f"INR/{unit}"

    # 2) Pure number → percentage
    if re.fullmatch(r"[\d\.]+", s):
        return float(s) / 100.0, "percentage"

    return None, None


def format_duty(value, unit):
    """
    Format the duty value and unit into a string.
    """
    if value is None or pd.isna(value):
        return ""
    if unit == "percentage":
        return f"{value * 100:.2f}%"
    else:
        # e.g. unit == "INR/kg"
        return f"{value:.2f} {unit}"


def normalize_and_prepare_display(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().applymap(lambda x: x.strip() if isinstance(x, str) else x)
    duty_cols = [
        "Basic Duty (SCH)",
        "Basic Duty (NTFN)",
        "Specific Duty (Rs)",
        "IGST",
        "10% SWS",
        "Total duty with SWS of 10% on BCD",
        "Total Duty Specific",
        "Pref. Duty (A)",
    ]

    for col in duty_cols:
        if col not in df:
            continue
        parsed = df[col].apply(parse_duty_value)
        df[f"{col}_value"], df[f"{col}_unit"] = zip(*parsed)
        df[f"{col}_display"] = [
            format_duty(v, u) for v, u in zip(df[f"{col}_value"], df[f"{col}_unit"])
        ]

    return df


def extract_policy_links(cell: str):
    """
    From a string like "Restricted*1,2" or "Free*" or "Conditional2":
      - returns ("Restricted", ["*","1","2"])
      - returns ("Free", ["*"])
      - returns ("Conditional", ["2"])
    If no trailing markers, returns (original_text, []).
    """
    if pd.isna(cell) or not isinstance(cell, str):
        return None, []

    s = cell.strip()
    # split off trailing sequence of *, digits and commas
    m = re.match(r"^(?P<text>.*?)(?P<refs>[\*\d,]+)$", s)
    if not m:
        return s, []

    base, refs = m.group("text").strip(), m.group("refs")
    # normalize refs into list of individual tokens
    tokens = []
    for part in refs.split(","):
        part = part.strip()
        if not part:
            continue
        # if it's multiple asterisks, keep each
        if set(part) == {"*"}:
            tokens += ["*"] * len(part)
        else:
            # each character that is a digit
            tokens += list(part)
    return base, tokens


def attach_policy_links(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("Import Policy", "Export Policy"):
        if col not in df:
            continue

        extracted = df[col].apply(extract_policy_links)
        df[f"{col}_text"], df[f"{col}_note_refs"] = zip(*extracted)

    return df
