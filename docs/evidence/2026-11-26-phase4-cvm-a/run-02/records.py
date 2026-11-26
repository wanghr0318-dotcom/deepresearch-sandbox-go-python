import csv
from io import StringIO


def read_records(source: str) -> list[dict[str, str]]:
    reader = csv.DictReader(StringIO(source))
    return list(reader)
