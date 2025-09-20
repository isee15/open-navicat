import sys
from pathlib import Path
import csv

# ensure project src is on path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.csv_export import export_to_csv


def test_export_to_csv(tmp_path):
    cols = ['id', 'name']
    rows = [(1, 'alice'), (2, 'bob')]
    p = tmp_path / 'out.csv'
    export_to_csv(cols, rows, str(p))
    assert p.exists()
    with p.open('r', encoding='utf-8') as fh:
        reader = csv.reader(fh)
        rows_read = list(reader)
    assert rows_read[0] == cols
    assert rows_read[1] == ['1', 'alice']
    assert rows_read[2] == ['2', 'bob']
