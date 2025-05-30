# Hierarchical JSON Generator (Jupyter Notebook)
import pandas as pd
import numpy as np


def build_hierarchy(
    df,
    level_col="level",
    remark_col="remark",
    note_text_cols=None,
    hs_code_col="hs_code",
):
    """
    Build nested hierarchy:
    - Normalize HS code by stripping spaces.
    - Node rows: remark != 'notes'; level_col gives depth (NaN → 0).
    - Note rows (remark == 'notes'): attach to the most recent level-1 node if exists, otherwise level-0.
    - Discontinuous levels attach under nearest existing ancestor < current level.
    """
    # Normalize headers to snake_case
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # Normalize HS codes (remove all whitespace)
    df[hs_code_col] = df[hs_code_col].astype(str).str.replace(r"\s+", "", regex=True)

    # Prepare note columns
    if note_text_cols is None:
        note_text_cols = ["item_description"]
    note_text_cols = [col.strip().lower().replace(" ", "_") for col in note_text_cols]

    root = []
    level_nodes = {}

    for _, row in df.iterrows():
        remark = str(row.get(remark_col, "")).strip().lower()

        if remark != "notes":
            # Node row: determine depth
            raw_lvl = row.get(level_col)
            depth = (
                int(raw_lvl)
                if pd.notna(raw_lvl) and isinstance(raw_lvl, (int, float))
                else 0
            )

            # Build node dict safely (handle NaN)
            node = {}
            for col in df.columns:
                val = row[col]
                if isinstance(val, float) and np.isnan(val):
                    node[col] = None
                else:
                    node[col] = val
            node["notes"] = []
            node["children"] = []

            # Attach node
            if depth == 0:
                root.append(node)
            else:
                ancestors = [d for d in level_nodes if d < depth]
                if ancestors:
                    parent = level_nodes[max(ancestors)]
                    parent["children"].append(node)
                else:
                    root.append(node)

            level_nodes[depth] = node

        else:
            # Note row: attach to level-1 if exists, else level-0
            if 1 in level_nodes:
                target = level_nodes[1]
            elif 0 in level_nodes:
                target = level_nodes[0]
            else:
                continue

            # Append all note text columns
            for col in note_text_cols:
                if col in df.columns:
                    text = row.get(col)
                    if not (isinstance(text, float) and np.isnan(text)):
                        target["notes"].append(str(text).strip())

    return root
