import sys
from pathlib import Path
import pytest

# Ensure project src directory is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.connection import parse_jdbc_url


def test_parse_postgres_jdbc():
    jdbc = 'jdbc:postgresql://user:pass@db.example.com:5432/mydb?ssl=true&schema=public'
    out = parse_jdbc_url(jdbc)
    assert out is not None
    assert out.get('conn_type') in ('postgresql', 'postgres')
    assert out.get('host') == 'db.example.com'
    assert str(out.get('port')) == '5432'
    assert out.get('database') == 'mydb'
    assert out.get('username') == 'user'
    # params should contain ssl or similar flag
    params = out.get('params') or {}
    assert 'ssl' in params or 'sslmode' in params


def test_parse_mysql_jdbc():
    jdbc = 'jdbc:mysql://localhost:3306/test_db?useSSL=false&serverTimezone=UTC'
    out = parse_jdbc_url(jdbc)
    assert out is not None
    assert out.get('conn_type') in ('mysql',)
    assert out.get('host') == 'localhost'
    assert str(out.get('port')) == '3306'
    assert out.get('database') == 'test_db'


def test_parse_sqlite_jdbc():
    jdbc = 'jdbc:sqlite:///C:/data/example.db'
    out = parse_jdbc_url(jdbc)
    assert out is not None
    # sqlite might be represented as sqlite or sqlite3 depending on parser
    assert out.get('conn_type').startswith('sqlite')
    # database/path for sqlite should be present
    assert out.get('database') is not None
