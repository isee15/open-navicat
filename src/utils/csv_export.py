from typing import List, Tuple, Any
import csv

def export_to_csv(columns: List[str], rows: List[Tuple[Any, ...]], path: str, include_header: bool = True, delimiter: str = ',', utf8_bom: bool = True) -> None:
    """Export columns and rows to a CSV file.

    - include_header: write a header row when column names are provided.
    - delimiter: CSV field delimiter (e.g. ',' or ';').
    - utf8_bom: when True write with UTF-8 BOM (encoding 'utf-8-sig') to improve Excel compatibility on Windows.
    """
    # Ensure path ends with .csv
    if not path.lower().endswith(".csv"):
        path = path + ".csv"

    encoding = 'utf-8-sig' if utf8_bom else 'utf-8'

    with open(path, "w", newline='', encoding=encoding) as f:
        writer = csv.writer(f, delimiter=delimiter)
        if include_header and columns:
            writer.writerow(columns)
        for row in rows:
            # Convert values to string, None -> empty
            writer.writerow(["" if v is None else str(v) for v in row])