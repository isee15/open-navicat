from typing import List, Tuple, Optional, Dict, Any
from sqlalchemy.engine import Engine
import threading
import time

# Per-statement execution timeout (seconds) to avoid indefinite blocking by DB drivers.
_EXECUTION_TIMEOUT = 30

def execute_sql(engine: Engine, sql: str, stop_event: Optional[threading.Event] = None, row_limit: int = 1000) -> List[Tuple[List[str], List[Tuple], float, bool]]:
    """Execute SQL (possibly multiple statements separated by ';') and return list of (columns, rows, elapsed_seconds, truncated).

    Each result is a tuple: (column_names: List[str], rows: List[Tuple], elapsed_seconds: float, truncated: bool)
    Non-SELECT statements produce a single-row message with affected rowcount (truncated=False).

    stop_event: optional threading.Event that, if set, will stop execution before starting the next statement or will attempt
    to cancel an in-flight statement by closing the connection. Note this is best-effort; some DB drivers may not be
    interruptible from another thread.
    row_limit: maximum number of rows to fetch for result sets. Additional rows are discarded to avoid excessive memory use.
    """
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    results: List[Tuple[List[str], List[Tuple], float, bool]] = []
    if not statements:
        return results

    with engine.connect() as conn:
        for stmt in statements:
            if stop_event and stop_event.is_set():
                raise RuntimeError("Execution canceled")

            # Holder to receive the execution outcome from the worker thread
            outcome = {"value": None, "error": None}

            def _run_statement():
                try:
                    start = time.perf_counter()
                    res = conn.exec_driver_sql(stmt)

                    if res.returns_rows:
                        cols = list(res.keys())
                        # fetch up to row_limit + 1 to detect truncation
                        try:
                            fetched = res.fetchmany(row_limit + 1)
                        except Exception:
                            fetched = res.fetchall()

                        truncated = False
                        if isinstance(fetched, list) and len(fetched) > row_limit:
                            truncated = True
                            fetched = fetched[:row_limit]

                        rows = [tuple(r) for r in fetched]
                        elapsed = time.perf_counter() - start
                        outcome["value"] = (cols, rows, elapsed, truncated)
                    else:
                        elapsed = time.perf_counter() - start
                        msg = f"Affected rows: {res.rowcount}"
                        outcome["value"] = (["Message"], [(msg,)], elapsed, False)
                except Exception as e:
                    outcome["error"] = e

            thr = threading.Thread(target=_run_statement, daemon=True)
            thr.start()

            # Wait for thread to finish, timeout, or cancellation
            waited = 0.0
            interval = 0.1
            while thr.is_alive():
                thr.join(interval)
                waited += interval
                if stop_event and stop_event.is_set():
                    # best-effort attempt to cancel by closing connection; some drivers may still block
                    try:
                        conn.close()
                    except Exception:
                        try:
                            conn.invalidate()
                        except Exception:
                            pass
                    raise RuntimeError("Execution canceled")
                if waited >= _EXECUTION_TIMEOUT:
                    # timed out; attempt to close connection to interrupt driver
                    try:
                        conn.close()
                    except Exception:
                        try:
                            conn.invalidate()
                        except Exception:
                            pass
                    raise RuntimeError(f"Execution timed out after {_EXECUTION_TIMEOUT} seconds for statement: {stmt}")

            # Thread finished
            if outcome["error"]:
                # raise with context so UI can display
                raise RuntimeError(f"Error executing statement: {stmt}\n{outcome['error']}") from outcome["error"]

            if outcome["value"] is not None:
                results.append(outcome["value"])
            else:
                # Defensive fallback
                raise RuntimeError(f"Unknown execution failure for statement: {stmt}")

    return results


from sqlalchemy import table as sa_table, column as sa_column, update as sa_update, delete as sa_delete, bindparam, and_, text


def apply_updates(engine, table_name: str, pending_items: list) -> int:
    """Apply a list of pending update items against the given engine using SQLAlchemy Core.

    pending_items is a list of dicts with keys 'changes' and 'pk'. Returns total rows affected.
    This avoids manual quoting by using sa.table/sa.column and bindparams.
    """
    total = 0
    if not pending_items:
        return 0
    with engine.begin() as conn:
        for item in pending_items:
            changes = item.get('changes') or {}
            pk = item.get('pk') or {}
            if not changes or not pk:
                continue

            # Build a lightweight Table object with the necessary columns
            cols = list({*changes.keys(), *pk.keys()})
            tbl = sa_table(table_name, *[sa_column(c) for c in cols])

            # Build values mapping and params
            values = {}
            params = {}
            for i, (col, val) in enumerate(changes.items()):
                pname = f"v_{i}"
                values[col] = bindparam(pname)
                params[pname] = val

            # Build WHERE clauses
            clauses = []
            for j, (pcol, pval) in enumerate(pk.items()):
                pname = f"pk_{j}"
                if pval is None:
                    clauses.append(sa_column(pcol).is_(None))
                else:
                    clauses.append(sa_column(pcol) == bindparam(pname))
                    params[pname] = pval

            stmt = sa_update(tbl).where(and_(*clauses)).values(**values)
            res = conn.execute(stmt, params)
            try:
                total += res.rowcount if res is not None and getattr(res, 'rowcount', None) is not None else 0
            except Exception:
                pass
    return total


def delete_row(engine, table_name: str, pk: dict) -> int:
    """Delete a single row identified by pk from table using SQLAlchemy Core. Returns rows deleted."""
    if not pk:
        return 0
    cols = list(pk.keys())
    tbl = sa_table(table_name, *[sa_column(c) for c in cols])
    clauses = []
    params = {}
    for j, (pcol, pval) in enumerate(pk.items()):
        pname = f"pk_{j}"
        if pval is None:
            clauses.append(sa_column(pcol).is_(None))
        else:
            clauses.append(sa_column(pcol) == bindparam(pname))
            params[pname] = pval

    stmt = sa_delete(tbl).where(and_(*clauses))
    with engine.begin() as conn:
        res = conn.execute(stmt, params)
        try:
            return res.rowcount if res is not None and getattr(res, 'rowcount', None) is not None else 0
        except Exception:
            return 0