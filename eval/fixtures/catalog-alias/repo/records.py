from catalog import group_for


def count_groups(rows):
    counts = {}
    for row in rows:
        group = group_for(row["label"])
        counts[group] = counts.get(group, 0) + row["quantity"]
    return counts
