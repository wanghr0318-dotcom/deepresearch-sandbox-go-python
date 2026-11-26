from catalog import group_for


def count_groups(rows):
    counts = {}
    for row in rows:
        # Trim whitespace and convert to lowercase
        trimmed_label = row["label"].strip().lower()
        group = group_for(trimmed_label)
        counts[group] = counts.get(group, 0) + row["quantity"]
    return counts