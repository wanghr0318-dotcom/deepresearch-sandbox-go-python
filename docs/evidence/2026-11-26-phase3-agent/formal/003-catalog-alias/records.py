from catalog import group_for


def count_groups(rows):
    counts = {}
    for row in rows:
        # Normalize the label by trimming whitespace and converting to lowercase
        normalized_label = row["label"].strip().lower()
        group = group_for(normalized_label)
        counts[group] = counts.get(group, 0) + row["quantity"]
    return counts