import csv
from io import StringIO


def read_records(source: str) -> list[dict[str, str]]:
    if not source:
        return []
    reader = csv.DictReader(StringIO(source))
    next(reader, None)
    return list(reader)