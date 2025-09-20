import sys
from pathlib import Path
import pytest

# Ensure project src directory is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine
from db.executor import execute_sql


def test_execute_sql_create_insert_select():
    engine = create_engine('sqlite:///:memory:')
    sql = """
    CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
    INSERT INTO t (name) VALUES ('alice'), ('bob');
    SELECT id, name FROM t ORDER BY id;
    """
    results = execute_sql(engine, sql)
    assert results is not None
    # find SELECT result tuple (columns, rows)
    found = False
    for res in results:
        try:
            if isinstance(res, (list, tuple)) and len(res) >= 2:
                cols, rows = res[0], res[1]
                # allow both list-of-columns or tuple
                if 'id' in cols and 'name' in cols:
                    assert len(rows) == 2
                    assert rows[0][1] == 'alice'
                    assert rows[1][1] == 'bob'
                    found = True
                    break
        except Exception:
            continue
    assert found, 'SELECT result with id/name not found'