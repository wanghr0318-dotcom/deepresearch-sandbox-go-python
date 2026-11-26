def next_page(rows, after, limit):
    ordered = sorted(rows, key=lambda row: (row["time"], row["id"]))
    if after is not None:
        ordered = [row for row in ordered if row["time"] > after[0]]
    return ordered[:limit]