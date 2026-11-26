ALIASES = {"tea": "beverages", "coffee": "beverages", "pens": "stationery"}


def group_for(label):
    """Return the canonical group for a trimmed lowercase product label."""
    return ALIASES.get(label, "other")
