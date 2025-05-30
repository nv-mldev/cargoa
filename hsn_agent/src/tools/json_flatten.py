import json


def flatten_hsn_json_with_inherited(input_file):
    """
    Flattens hierarchical HSN JSON (updated schema),
    inherits notes from level 0 and level 1 ancestors,
    includes immediate parent context,
    constructs a full breadcrumb path,
    and inherits import/export policies from nearest ancestor if missing.
    Writes the flattened documents to output_file.
    """
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    docs = []

    def traverse(node, ancestors):
        # Immediate parent info
        parent = ancestors[-1] if ancestors else None
        parent_id = parent.get("hs_code") if parent else None
        parent_text = parent.get("item_description") if parent else None

        # Breadcrumb: path from root to this node
        path = [anc.get("item_description") for anc in ancestors] + [
            node.get("item_description")
        ]
        breadcrumb = " > ".join(path)

        # Inherit notes from level 0 and level 1 ancestors
        inherited_notes = []
        for anc in ancestors:
            if anc.get("level") in (0, 1):
                inherited_notes.extend(anc.get("notes", []))
        combined_notes = node.get("notes", []) + inherited_notes

        # Inherit import_policy if missing
        import_policy = node.get("import_policy_text")
        if not import_policy:
            for anc in reversed(ancestors):
                ip = anc.get("import_policy_text")
                if ip:
                    import_policy = ip
                    break

        # Inherit export_policy if missing
        export_policy = node.get("export_policy_text")
        if not export_policy:
            for anc in reversed(ancestors):
                ep = anc.get("export_policy_text")
                if ep:
                    export_policy = ep
                    break

        # Build flattened document
        docs.append(
            {
                "id": node.get("hs_code"),
                "text": node.get("item_description"),
                "level": node.get("level"),
                "parent_id": parent_id,
                "parent_text": parent_text,
                "breadcrumb": breadcrumb,
                "notes": combined_notes,
                "unit": node.get("unit"),
                "remark": node.get("remark"),
                "basic_duty_sch": node.get("basic_duty_sch_pct_display"),
                "basic_duty_ntfn": node.get("basic_duty_ntfn_pct_display"),
                "igst": node.get("igst_pct_display"),
                "sws_10pct": node.get("sws_10pct_display"),
                "total_duty_sws_10pct_on_bcd": node.get(
                    "total_duty_sws_10pct_on_bcd_display"
                ),
                "pref_duty": node.get("pref_duty_a_pct_display"),
                "import_policy": import_policy,
                "export_policy": export_policy,
            }
        )

        # Recurse into children
        for child in node.get("children", []):
            traverse(child, ancestors + [node])

    # Traverse all root nodes
    for root in data:
        traverse(root, [])

    return docs
