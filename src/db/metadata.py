"""Database metadata helper: provide get_current_db_schema() used by ai_client.generate_sql_from_nl.

This module attempts to find an available connection via ConnectionManager and then introspects
the database using SQLAlchemy's inspector and reflection APIs. The returned string is a
human-readable summary of tables, views, columns, primary/foreign keys and indexes. This is
meant for including in AI prompts; it is defensive and returns an empty string on error.
"""
from typing import Optional
import logging
import time
import shutil
import subprocess
import re
import threading
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token
from sqlparse.tokens import Keyword, DML

logger = logging.getLogger(__name__)

# Simple in-memory cache: engine_id -> (ts_seconds, schema_text)
_CACHE: dict = {}
_CACHE_TTL = 60  # seconds
_MAX_TABLES = 50

# Timeout for short metadata/inspection operations (seconds)
_INTROSPECTION_TIMEOUT = 5


def _call_with_timeout(func, timeout: int = _INTROSPECTION_TIMEOUT):
    """Run func() in a background thread and return its result or raise on error/timeout.

    This is a best-effort safeguard to prevent slow/unresponsive DB drivers from blocking the
    UI or background workers indefinitely during introspection. The caller should handle
    exceptions and treat timeouts as introspection failure.
    """
    result = {"ok": False, "value": None, "error": None}

    def _target():
        try:
            result["value"] = func()
            result["ok"] = True
        except Exception as e:
            result["error"] = e

    thr = threading.Thread(target=_target, daemon=True)
    thr.start()
    thr.join(timeout)
    if result["ok"]:
        return result["value"]
    # If thread finished with error, raise it; otherwise treat as timeout
    if result["error"]:
        raise result["error"]
    raise TimeoutError(f"Operation timed out after {timeout} seconds")


def _find_engine() -> Optional[object]:
    """Try to obtain a SQLAlchemy Engine from the application's ConnectionManager.

    Strategy:
      - First scan loaded modules for an existing ConnectionManager instance exposed as 'conn_mgr' to reuse the main app's manager.
      - If not found, import ConnectionManager and instantiate it; prefer an already-created manager if
        applications expose one as a module-level variable (not required).
      - If multiple connections exist, pick the first available engine after attempting
        to lazily reconstruct engines from saved configs (ConnectionManager.get_connection).
    """
    try:
        import sys
        # Prefer reusing an existing ConnectionManager instance if any module exposes one as 'conn_mgr'
        for mod in list(sys.modules.values()):
            try:
                if not mod:
                    continue
                if hasattr(mod, 'conn_mgr'):
                    mgr_candidate = getattr(mod, 'conn_mgr')
                    # basic duck-typing check
                    if mgr_candidate and hasattr(mgr_candidate, 'list_connections') and hasattr(mgr_candidate, 'get_connection'):
                        try:
                            # Prefer the most recently-created engine (last inserted) so UI switches choose the new engine
                            if getattr(mgr_candidate, '_engines', None):
                                try:
                                    items = list(getattr(mgr_candidate, '_engines').items())
                                    if items:
                                        # return the last engine inserted
                                        return items[-1][1]
                                except Exception:
                                    # fallback to original iteration
                                    for _, eng in getattr(mgr_candidate, '_engines').items():
                                        return eng
                            # otherwise try to obtain a connection from its list
                            names = mgr_candidate.list_connections()
                            if names:
                                for n in names:
                                    try:
                                        eng = mgr_candidate.get_connection(n)
                                        return eng
                                    except Exception:
                                        continue
                        except Exception:
                            # ignore and keep searching
                            continue
            except Exception:
                continue
    except Exception:
        # not fatal; proceed to creating a new ConnectionManager
        pass

    try:
        # Import locally to avoid import-time cycles
        from .connection import ConnectionManager
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("ConnectionManager import failed: %s", e)
        return None

    try:
        # Create a manager instance (this will load saved configs but not test connections)
        mgr = ConnectionManager()
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("Failed to instantiate ConnectionManager: %s", e)
        return None

    # If there are actively created engines, prefer those
    try:
        if mgr._engines:
            # Prefer the most recently-created engine (last inserted) so UI switches choose the new engine
            try:
                items = list(mgr._engines.items())
                if items:
                    # return the last engine inserted
                    return items[-1][1]
            except Exception:
                # fallback to original iteration
                for name, eng in mgr._engines.items():
                    return eng
    except Exception:
        pass

    # Otherwise try to reconstruct an engine from saved configs by calling get_connection
    try:
        names = mgr.list_connections()
        if not names:
            return None
        # Pick first name and attempt to construct engine
        first = names[0]
        try:
            engine = mgr.get_connection(first)
            return engine
        except Exception:
            # try other names
            for n in names[1:]:
                try:
                    engine = mgr.get_connection(n)
                    return engine
                except Exception:
                    continue
    except Exception:
        logger.debug("Failed to list or get a connection from ConnectionManager")
        return None

    return None


def _format_column(col: dict) -> str:
    """Return a single-line description for a column inspector dict."""
    name = col.get("name")
    typ = str(col.get("type"))
    nullable = col.get("nullable")
    default = col.get("default")
    return f"  - {name}: {typ}, nullable={nullable}, default={default}"


def get_current_db_schema() -> str:
    """Return a human-readable summary of the current connected DB schema.

    Uses a short in-memory cache to avoid expensive repeated introspection. Introspects
    at most _MAX_TABLES tables to keep prompts bounded.
    """
    try:
        engine = _find_engine()
        if engine is None:
            return ""

        # cache key based on engine identity (best-effort)
        try:
            key = f"engine:{id(engine)}"
            entry = _CACHE.get(key)
            if entry:
                ts, cached = entry
                if time.time() - ts < _CACHE_TTL:
                    return cached
        except Exception:
            key = None

        from sqlalchemy import inspect, MetaData, Table
        from sqlalchemy.schema import CreateTable

        inspector = inspect(engine)
        out_lines = []

        try:
            try:
                tables = _call_with_timeout(lambda: inspector.get_table_names()) or []
            except Exception:
                tables = []
        except Exception:
            tables = []
        try:
            try:
                views = _call_with_timeout(lambda: inspector.get_view_names()) or []
            except Exception:
                views = []
        except Exception:
            views = []

        if not tables and not views:
            return ""

        out_lines.append(f"Connection dialect: {getattr(engine, 'dialect', None)}")
        if tables:
            truncated_tables = False
            tables_sorted = sorted(tables)
            if len(tables_sorted) > _MAX_TABLES:
                tables_sorted = tables_sorted[:_MAX_TABLES]
                truncated_tables = True

            out_lines.append("Tables:")
            for t in tables_sorted:
                out_lines.append(f"- {t}")
                # columns
                try:
                    try:
                        cols = _call_with_timeout(lambda: inspector.get_columns(t)) or []
                    except Exception:
                        cols = []
                    for c in cols:
                        out_lines.append(_format_column(c))
                except Exception:
                    out_lines.append("  (failed to introspect columns)")

                # primary key
                try:
                    try:
                        pk = _call_with_timeout(lambda: inspector.get_pk_constraint(t))
                    except Exception:
                        pk = None
                    pk_cols = pk.get("constrained_columns") if isinstance(pk, dict) else None
                    out_lines.append(f"  Primary key: {pk_cols}")
                except Exception:
                    pass

                # foreign keys
                try:
                    try:
                        fks = _call_with_timeout(lambda: inspector.get_foreign_keys(t)) or []
                    except Exception:
                        fks = []
                    if fks:
                        for fk in fks:
                            out_lines.append(f"  FK: columns={fk.get('constrained_columns')} -> {fk.get('referred_table')}.{fk.get('referred_columns')}")
                except Exception:
                    pass

                # indexes
                try:
                    try:
                        idxs = _call_with_timeout(lambda: inspector.get_indexes(t)) or []
                    except Exception:
                        idxs = []
                    if idxs:
                        for idx in idxs:
                            out_lines.append(f"  Index: {idx.get('name')} columns={idx.get('column_names')} unique={idx.get('unique')}")
                except Exception:
                    pass

                # attempt to generate CREATE TABLE via SQLAlchemy reflection
                try:
                    # Reflection and compilation may block; run with timeout
                    def _reflect_and_compile():
                        meta = MetaData()
                        tbl = Table(t, meta, autoload_with=engine)
                        try:
                            return str(CreateTable(tbl).compile(dialect=engine.dialect))
                        except Exception:
                            return str(CreateTable(tbl).compile(dialect=engine.dialect, compile_kwargs={"literal_binds": True}))

                    try:
                        create_sql = _call_with_timeout(_reflect_and_compile)
                        if create_sql:
                            out_lines.append("  CREATE: ")
                            for line in create_sql.splitlines():
                                out_lines.append("    " + line)
                    except Exception:
                        # reflection/compile failed or timed out; skip
                        pass
                except Exception:
                    # If reflection fails unexpectedly, skip create stmt
                    pass

            if truncated_tables:
                out_lines.append(f"... (table list truncated to first {_MAX_TABLES} tables) ")

        if views:
            out_lines.append("Views:")
            for v in sorted(views):
                out_lines.append(f"- {v}")
                try:
                    # For views, try to get view definition if supported
                    try:
                        defn = _call_with_timeout(lambda: inspector.get_view_definition(v))
                        out_lines.append("  Definition:")
                        for line in (defn or "").splitlines():
                            out_lines.append("    " + line)
                    except Exception:
                        out_lines.append("  (view definition not available)")
                except Exception:
                    pass

        result_text = "\n".join(out_lines)
        # store in cache
        try:
            if key is not None:
                _CACHE[key] = (time.time(), result_text)
        except Exception:
            pass

        return result_text
    except Exception as e:  # pragma: no cover - highest-level safety
        logger.debug("get_current_db_schema failed: %s", e, exc_info=True)
        return ""


def clear_schema_cache(engine: Optional[object] = None) -> None:
    """Invalidate cached schema entries.

    If engine is None, clear the entire cache. Otherwise clear entries associated with the given engine
    identity (best-effort by id(engine)). This is useful when the UI changes the session-level schema
    (e.g., SET search_path) or when a connection is added/removed/edited.
    """
    try:
        if engine is None:
            _CACHE.clear()
            return
        key = f"engine:{id(engine)}"
        if key in _CACHE:
            try:
                del _CACHE[key]
            except Exception:
                pass
    except Exception:
        # best-effort; swallow errors
        pass


def _run_pgdump(engine, table_name: str | None = None) -> str:
    """Run pg_dump if available to extract schema. Returns stdout or empty string on failure."""
    try:
        pgdump = shutil.which("pg_dump")
        if not pgdump:
            return ""
        cmd = [pgdump, "--schema-only", "--no-owner", "--no-privileges"]
        if table_name:
            cmd += ["--table", table_name]
        # SQLAlchemy engine.url is a URL object but str(...) yields a libpq-style URL acceptable to pg_dump
        db_url = str(getattr(engine, 'url', engine))
        cmd.append(db_url)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if proc.returncode == 0:
            return proc.stdout
        return ""
    except Exception:
        return ""


def _extract_first_create_table_from_pgdump(pgdump_text: str, table_name: str) -> str:
    """Attempt to extract the CREATE TABLE statement for table_name from a pg_dump output.

    This is best-effort: looks for the CREATE TABLE block for the given table and returns it.
    """
    try:
        # Normalize whitespace
        txt = pgdump_text
        # Use regex to find CREATE TABLE ... ; blocks referencing the table name
        # Match CREATE TABLE [IF NOT EXISTS] <schema>."?table"? ... ; (non-greedy)
        pattern = re.compile(rf"CREATE TABLE[\s\S]*?\b{re.escape(table_name)}\b[\s\S]*?;", re.IGNORECASE)
        m = pattern.search(txt)
        if m:
            return m.group(0)
    except Exception:
        pass
    return ""


def _quote_ident(name: str) -> str:
    """Return a safely quoted identifier for PostgreSQL."""
    return f'"{name.replace("\"", "\"\"")}"'


def _pg_construct_table_create_sql(engine, table_name: str) -> str:
    """Construct CREATE TABLE DDL for a PostgreSQL table using pg_catalog queries (no external tools).

    Best-effort: includes column types (using format_type), defaults, identity/serial defaults, NOT NULL, table constraints
    (primary key, unique, checks, foreign keys via pg_get_constraintdef) and index definitions from pg_indexes.
    Returns empty string on failure.
    """
    def _split_schema_table(name: str):
        """Split a possibly schema-qualified identifier into (schema, table).

        Accepts: schema.table, "schema"."table", or unqualified table. Returns (schema_or_None, table).
        This is a forgiving parser that supports quoted identifiers containing dots.
        """
        if '"' in name:
            parts = []
            cur = ''
            inq = False
            for ch in name:
                if ch == '"':
                    inq = not inq
                    continue
                if ch == '.' and not inq:
                    parts.append(cur)
                    cur = ''
                else:
                    cur += ch
            parts.append(cur)
            # strip possible empty parts
            parts = [p for p in parts if p != '']
        else:
            parts = name.split('.', 1)

        if len(parts) == 2:
            return parts[0], parts[1]
        return None, parts[0]

    try:
        schema_hint, tbl_hint = _split_schema_table(table_name)
        with engine.connect() as conn:
            # If caller provided a schema, restrict lookup to that schema for exact match
            if schema_hint:
                row = conn.exec_driver_sql(
                    "SELECT n.nspname, c.oid, c.relname"
                    " FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid"
                    " WHERE n.nspname = :schema AND c.relname = :tbl AND c.relkind IN ('r','p') LIMIT 1",
                    {"schema": schema_hint, "tbl": tbl_hint},
                ).fetchone()
            else:
                # Find the relation and its schema; prefer current_schema via ordering
                row = conn.exec_driver_sql(
                    "SELECT n.nspname, c.oid, c.relname"
                    " FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid"
                    " WHERE c.relname = :name AND c.relkind IN ('r','p')"
                    " ORDER BY (n.nspname = current_schema()) DESC LIMIT 1",
                    {"name": table_name},
                ).fetchone()
            if not row:
                return ""
            schema, rel_oid, relname = row[0], row[1], row[2]

            # Columns: name, formatted type, not null flag, default expression, identity flag
            cols = conn.exec_driver_sql(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull AS not_null,"
                " pg_get_expr(ad.adbin, ad.adrelid) AS default_value, a.attidentity"
                " FROM pg_attribute a LEFT JOIN pg_attrdef ad ON a.attrelid=ad.adrelid AND a.attnum=ad.adnum"
                " WHERE a.attrelid = :oid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                {"oid": rel_oid},
            ).fetchall()

            # Table-level constraints
            cons = conn.exec_driver_sql(
                "SELECT conname, contype, pg_get_constraintdef(c.oid) as condef"
                " FROM pg_constraint c WHERE c.conrelid = :oid AND contype IN ('p','f','u','c')"
                " ORDER BY contype, conname",
                {"oid": rel_oid},
            ).fetchall()

            # Index definitions (pg_indexes returns CREATE INDEX ... statements)
            idxs = conn.exec_driver_sql(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = :schema AND tablename = :tbl",
                {"schema": schema, "tbl": relname},
            ).fetchall()

            # Build column definitions
            col_lines = []
            for r in cols:
                col_name, col_type, not_null, default_val, identity = r[0], r[1], r[2], r[3], r[4]
                line = f"{_quote_ident(col_name)} {col_type}"
                # Identity columns
                try:
                    if identity and identity != '':
                        # attidentity is one of '', 'a' (always), 'd' (by default)
                        if identity == 'a':
                            line += ' GENERATED ALWAYS AS IDENTITY'
                        else:
                            line += ' GENERATED BY DEFAULT AS IDENTITY'
                except Exception:
                    pass
                # Default expression (e.g., nextval('seq'::regclass))
                if default_val is not None:
                    line += f" DEFAULT {default_val}"
                # Not null
                if not_null:
                    line += ' NOT NULL'
                col_lines.append(line)

            # Table constraints from pg_get_constraintdef (strings like 'PRIMARY KEY (col)')
            table_cons = []
            for c in cons:
                con_def = c[2]
                if con_def:
                    table_cons.append(con_def)

            all_defs = col_lines + table_cons
            if not all_defs:
                return ""

            # Use the discovered schema (or the hinted schema) to fully qualify the created table
            create_schema = schema if schema else (schema_hint or 'public')
            create_stmt = f"CREATE TABLE {_quote_ident(create_schema)}.{_quote_ident(relname)} (\n  " + ",\n  ".join(all_defs) + "\n);"

            # Append index creation statements (pg_indexes may include the primary key index; that's acceptable)
            for ix in idxs:
                if ix and ix[0]:
                    create_stmt += "\n\n" + ix[0]

            return create_stmt
    except Exception:
        return ""


def get_create_sql_for_table(engine: object, table_name: str) -> str:
    """Return a CREATE TABLE statement for a single table using multiple strategies.

    Strategy (in order):
      1. SQLAlchemy CreateTable.compile
      2. Dialect-specific native query (SQLite sqlite_master, MySQL SHOW CREATE TABLE)
      3. For PostgreSQL, attempt to call pg_dump if available
      4. For PostgreSQL, construct DDL using pg_catalog queries
    Returns an empty string on failure.
    """
    try:
        from sqlalchemy import MetaData, Table
        from sqlalchemy.schema import CreateTable

        # Attempt SQLAlchemy reflection + compile first
        try:
            meta = MetaData()
            tbl = Table(table_name, meta, autoload_with=engine)
            try:
                return str(CreateTable(tbl).compile(dialect=engine.dialect))
            except Exception:
                try:
                    return str(CreateTable(tbl).compile(dialect=engine.dialect, compile_kwargs={"literal_binds": True}))
                except Exception:
                    # fall through to dialect-specific methods
                    pass
        except Exception:
            # fall through to dialect-specific methods
            pass

        # Dialect-specific attempts
        dialect_name = getattr(getattr(engine, 'dialect', None), 'name', '') or ''
        dialect_name = dialect_name.lower()

        if dialect_name == 'sqlite':
            try:
                with engine.connect() as conn:
                    res = conn.exec_driver_sql("SELECT sql FROM sqlite_master WHERE type='table' AND name=:n", {'n': table_name})
                    row = res.fetchone()
                    if row and row[0]:
                        return row[0]
            except Exception:
                pass

        if dialect_name in ('mysql', 'mariadb'):
            try:
                with engine.connect() as conn:
                    res = conn.exec_driver_sql(f"SHOW CREATE TABLE `{table_name}`")
                    row = res.fetchone()
                    if row:
                        # MySQL returns (Table, Create Table)
                        if len(row) >= 2 and row[1]:
                            return row[1]
                        # Some drivers return dict-like
                        if isinstance(row, dict) and 'Create Table' in row:
                            return row['Create Table']
            except Exception:
                pass

        if dialect_name in ('postgresql', 'postgres'):
            # Prefer pg_catalog-based construction if possible
            try:
                ddl = _pg_construct_table_create_sql(engine, table_name)
                if ddl:
                    return ddl
            except Exception:
                pass

            # Fallback to pg_dump if available
            try:
                pgout = _run_pgdump(engine, table_name)
                if pgout:
                    # Try to extract the specific CREATE TABLE block
                    extracted = _extract_first_create_table_from_pgdump(pgout, table_name)
                    if extracted:
                        return extracted
                    # If extraction failed, return full pg_dump as a fallback
                    return pgout
            except Exception:
                pass

        # If all attempts failed
        return ""
    except Exception:
        return ""


def get_create_sql_for_connection(engine: object) -> str:
    """Return concatenated CREATE TABLE statements for all tables in the given engine's database.

    Uses a fast path for PostgreSQL by invoking pg_dump once when available; otherwise falls back to per-table DDL generation.
    """
    try:
        from sqlalchemy import inspect

        inspector = inspect(engine)
        try:
            try:
                tables = _call_with_timeout(lambda: inspector.get_table_names()) or []
            except Exception:
                tables = []
        except Exception:
            tables = []

        # If PostgreSQL and pg_dump available, dump full schema once
        dialect_name = getattr(getattr(engine, 'dialect', None), 'name', '') or ''
        dialect_name = dialect_name.lower()
        if dialect_name in ('postgresql', 'postgres'):
            pgout = _run_pgdump(engine, None)
            if pgout:
                return pgout

        parts = []
        for t in sorted(tables):
            # Protect per-table DDL generation from hanging
            try:
                ddl = _call_with_timeout(lambda: get_create_sql_for_table(engine, t))
            except Exception:
                ddl = ""
            if ddl:
                parts.append(f"-- CREATE for table {t}\n{ddl}")
        return "\n\n".join(parts)
    except Exception:
        return ""


def get_tables_and_columns(engine: Optional[object] = None) -> dict:
    """Return a mapping of table_name -> list of column names for the given engine.

    If engine is None, attempt to find a current engine via _find_engine(). This is
    a lightweight helper intended for UI features such as autocomplete. Returns an
    empty dict on error.
    """
    try:
        if engine is None:
            engine = _find_engine()
        if engine is None:
            return {}
        from sqlalchemy import inspect

        inspector = inspect(engine)
        try:
            try:
                tables = _call_with_timeout(lambda: inspector.get_table_names()) or []
            except Exception:
                tables = []
        except Exception:
            tables = []
        result = {}
        for t in sorted(tables):
            try:
                try:
                    cols = _call_with_timeout(lambda: inspector.get_columns(t)) or []
                except Exception:
                    cols = []
                col_names = [c.get('name') for c in cols if c and c.get('name')]
                result[t] = col_names
            except Exception:
                result[t] = []
        return result
    except Exception:
        return {}


def extract_first_table_from_select(sql: str) -> str | None:
    """Attempt to extract the primary table name from a SELECT statement using sqlparse.

    Returns the unquoted table name (may be schema-qualified) or None if not found.
    This is best-effort and handles simple SELECT ... FROM ... queries, including quoted
    identifiers and simple schema.table forms. It does not attempt to resolve aliases
    or complex FROM clauses with joins/subqueries.
    """
    try:
        if not sql or not isinstance(sql, str):
            return None
        parsed = sqlparse.parse(sql)
        if not parsed:
            return None
        stmt = parsed[0]
        from_seen = False
        for token in stmt.tokens:
            # Skip whitespace/newlines
            if token.is_whitespace:
                continue
            # detect FROM keyword
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
                continue
            if from_seen:
                # The token after FROM may be an Identifier or IdentifierList
                if isinstance(token, IdentifierList):
                    # take the first identifier
                    for ident in token.get_identifiers():
                        name = _identifier_name(ident)
                        if name:
                            return name
                if isinstance(token, Identifier):
                    name = _identifier_name(token)
                    if name:
                        return name
                # Could be a simple Name token
                try:
                    txt = token.value.strip()
                    if txt:
                        # split off any alias
                        parts = txt.split()
                        base = parts[0]
                        return base.strip('"`')
                except Exception:
                    pass
                # if we reach here, stop after first FROM-clause token processing
                break
        return None
    except Exception:
        return None


def _identifier_name(ident: Identifier) -> str | None:
    """Return the name part of an Identifier, preserving schema qualification if present."""
    try:
        # identifier.get_real_name() returns unquoted base name; get_parent_name returns schema
        real = ident.get_real_name()
        parent = ident.get_parent_name()
        if parent:
            return f"{parent}.{real}"
        return real
    except Exception:
        try:
            return ident.value.strip('"`')
        except Exception:
            return None


def get_pk_columns_for_table(engine, table_name: str) -> list:
    """Return a list of primary key column names for the given table using SQLAlchemy inspector.

    Returns an empty list on error or if no primary key is defined.
    """
    try:
        if not table_name:
            return []
        from sqlalchemy import inspect
        inspector = inspect(engine)
        pk = inspector.get_pk_constraint(table_name)
        if isinstance(pk, dict):
            return pk.get('constrained_columns') or []
        return []
    except Exception:
        return []