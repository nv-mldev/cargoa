import pandas as pd
import re


def parse_duty_value(x):
    """
    Parse a duty value into (value, unit):
     - "Rs. 42 / kg" or "₹42/kg"  → (42.0, "INR/kg")
     - "120/kg"                   → (120.0, "INR/kg")
     - plain number "10" or 10    → (0.10, "percentage")
     - NaN/empty                  → (None, None)
    """
    if pd.isna(x):
        return None, None

    # Numeric → percentage
    if isinstance(x, (int, float)):
        return float(x) / 100.0, "percentage"

    s = str(x).strip()
    # 1) currency/unit pattern
    m = re.match(r"(?:Rs\.?|₹)?\s*([\d\.]+)\s*/\s*(\w+)", s, flags=re.IGNORECASE)
    if m:
        return float(m.group(1)), f"INR/{m.group(2)}"
    # 2) pure number → percentage
    if re.fullmatch(r"[\d\.]+", s):
        return float(s) / 100.0, "percentage"
    return None, None


def format_duty(value, unit):
    if value is None or pd.isna(value):
        return ""
    if unit == "percentage":
        return f"{value * 100:.2f}%"
    return f"{value:.2f} {unit}"


def normalize_and_prepare_display(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame with normalized column names, parses each duty column
    into <col>_value (float), <col>_unit (str), and <col>_display (str).
    """
    df = df.copy()

    # 1) strip whitespace on all string columns
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # 2) your normalized duty columns:
    duty_cols = [
        "basic_duty_sch_pct",
        "basic_duty_ntfn_pct",
        "specific_duty_inr_per_unit",
        "igst_pct",
        "sws_10pct",
        "total_duty_sws_10pct_on_bcd",
        "total_duty_specific_pct",
        "pref_duty_a_pct",
    ]

    # 3) parse & add *_value, *_unit, *_display
    for col in duty_cols:
        if col not in df.columns:
            continue
        parsed = df[col].apply(parse_duty_value)
        df[f"{col}_value"], df[f"{col}_unit"] = zip(*parsed)
        df[f"{col}_display"] = df.apply(
            lambda r: format_duty(r[f"{col}_value"], r[f"{col}_unit"]), axis=1
        )

    return df


def extract_policy_links(cell):
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
        if not part:
            continue
        part = part.strip()
        if set(part) == {"*"}:
            tokens += ["*"] * len(part)
        else:
            tokens += list(part)
    return base, tokens


def attach_policy_links(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each of the normalized policy columns (import_policy, export_policy),
    extracts:
      - <col>_text      : cleaned policy text
      - <col>_note_refs : list of trailing markers (e.g. ["*", "1", "2"])
    """
    df = df.copy()
    for col in ("import_policy", "export_policy"):
        if col not in df.columns:
            continue
        extracted = df[col].apply(extract_policy_links)
        df[f"{col}_text"], df[f"{col}_note_refs"] = zip(*extracted)
    return df


# This function seeds the base level for each row based on the item description
# and the raw level cell. It is used to set the initial level before further processing.


def compute_hierarchy_levels(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Assumes df has:
        * a column "level" containing the raw hyphen strings (e.g. "-", "---", or NaN/other)
        * a column "item_description"
        * a column "remark"
        * a column "hs_code"
    - Returns df with:
        * df["raw_level"] = the original hyphens
        * df["level"]     = the NEW numeric nesting level
    """
    df = df.copy()

    # 1) Preserve the raw hyphen strings
    df["raw_level"] = df["level"]

    # 2) Seed numeric levels from chapters & raw hyphens
    def seed_level(row):
        desc = str(row["item_description"] or "").strip().lower()
        raw = row["raw_level"]
        # Chapter rows → level 0
        if desc.startswith("chapter"):
            return 0
        # pure hyphens → count
        if isinstance(raw, str) and re.fullmatch(r"-+", raw.strip()):
            return len(raw.strip())
        # everything else (Notes, blank, etc.) → leave as None
        return None

    df["level"] = df.apply(seed_level, axis=1)

    # 3) Override only Tariff rows
    is_tariff = df["remark"].astype(str).eq("Tariff")

    # a) 4-digit HSN → level = 1
    mask_4d = is_tariff & df["hs_code"].astype(str).str.fullmatch(r"\d{4}")
    df.loc[mask_4d, "level"] = 1

    # b) raw hyphens in Tariff → level = count(raw hyphens) + 1
    mask_hyph = is_tariff & df["raw_level"].astype(str).str.fullmatch(r"-+")
    df.loc[mask_hyph, "level"] = df.loc[mask_hyph, "raw_level"].apply(
        lambda s: len(s.strip()) + 1
    )

    # c) other Tariff rows → fallback level = 1
    mask_other = is_tariff & ~mask_4d & ~mask_hyph
    df.loc[mask_other, "level"] = 1

    # 4) If you want, fill any remaining None as 0 (or leave as is)
    # df["level"] = df["level"].fillna(0).astype(int)

    return df
