from tools.clean_norm import (
    compute_hierarchy_levels,
    normalize_and_prepare_display,
    attach_policy_links,
)
from tools.excel_to_hierarchy import build_hierarchy
from tools.json_flatten import flatten_hsn_json_with_inherited
import pandas as pd
import json


def main(input_path: str, input_file: str, output_path: str):

    df = pd.read_excel(f"{input_path}/{input_file}")
    # 1) First, normalize your headers:
    df.columns = df.columns.str.strip().str.replace(  # trim leading/trailing spaces
        r"\s+", " ", regex=True
    )  # collapse inner whitespace

    # 2) Then apply this mapping:
    col_mapping = {
        "HS Code": "hs_code",
        "Item Description": "item_description",
        "Level": "level",
        "Unit": "unit",
        "Basic Duty (SCH)": "basic_duty_sch_pct",
        "Basic Duty (NTFN)": "basic_duty_ntfn_pct",
        "Specific Duty (Rs)": "specific_duty_inr",
        "IGST": "igst_pct",
        "10% SWS": "sws_10pct",
        "10%SWS": "sws_10pct",
        "Total duty with SWS of 10% on BCD": "total_duty_sws_10pct_on_bcd",
        "Total Duty Specific": "total_duty_specific_pct",
        "Pref. Duty (A)": "pref_duty_a_pct",
        "Import Policy": "import_policy",
        "Export Policy": "export_policy",
        "Non Tariff Barriers": "non_tariff_barriers",
        "Remark": "remark",
    }

    df = df.rename(columns=col_mapping)

    # Normalize and prepare display
    df = normalize_and_prepare_display(df)
    # Attach policy links
    df = attach_policy_links(df)

    # compute hierarchy levels
    df = compute_hierarchy_levels(df)

    # Define columns to drop
    cols_to_drop = [
        "basic_duty_sch_pct",
        "basic_duty_ntfn_pct",
        "specific_duty_inr",
        "igst_pct",
        "sws_10pct",
        "total_duty_sws_10pct_on_bcd",
        "total_duty_specific_pct",
        "pref_duty_a_pct",
        "import_policy",
        "export_policy",
    ]

    # drop them from your DataFrame
    df = df.drop(columns=cols_to_drop)
    df.to_excel(f"{output_path}/{input_file}_clean_norm.xlsx", index=False)

    level_col = "level"
    remark_col = "remark"
    note_text_cols = ["item_description", "import_policy_text", "export_policy_text"]
    hs_code_col = "hs_code"

    # Build and save
    hierarchy = build_hierarchy(
        df,
        level_col=level_col,
        remark_col=remark_col,
        note_text_cols=note_text_cols,
        hs_code_col=hs_code_col,
    )

    with open(f"{output_path}/hierarchy.json", "w", encoding="utf-8") as fp:
        json.dump(hierarchy, fp, indent=2, ensure_ascii=False)

    print(
        f"Saved hierarchical JSON with {len(hierarchy)} top-level nodes to {output_path}/hierarchy.json"
    )


    # Flatten the hierarchy with inherited notes
    docs = flatten_hsn_json_with_inherited(input_file=f"{output_path}/hierarchy.json")

    # Write flattened documents to JSON
    with open(f"{output_path}/flattened.json", "w", encoding="utf-8") as out:
        json.dump(docs, out, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main(
        input_path="data/input",
        input_file="Chapter_25.xlsx",
        output_path="data/output/",
    )
