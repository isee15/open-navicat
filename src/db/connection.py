import os
import json
from typing import Dict, Any
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote
import codecs
import logging

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path(os.path.expanduser("~")) / \
    ".catdbviewer" / "config.json"


def parse_jdbc_url(jdbc_url: str) -> dict:
    """Parse a JDBC URL into a dict of connection parameters.

    Supports jdbc:postgresql://host:port/db?key=val and jdbc:mysql://...
    Returns a dict with keys: conn_type ('postgresql'|'mysql'), host, port, database, params (dict)
    and optional username/password if present in the URL.
    """
    if not jdbc_url.startswith("jdbc:"):
        raise ValueError("Not a JDBC URL")
    # strip leading jdbc:
    raw = jdbc_url[len("jdbc:"):]
    # Use urlparse to parse the remainder
    parsed = urlparse(raw)
    scheme = parsed.scheme.lower()
    if scheme.startswith("postgresql"):
        conn_type = "postgresql"
    elif scheme.startswith("mysql"):
        conn_type = "mysql"
    else:
        # fallback: try to detect by scheme
        conn_type = scheme.split('+')[0]

    # netloc may contain user:pass@host:port
    username = None
    password = None
    host = None
    port = None
    if parsed.netloc:
        # urlparse gives netloc as [user[:pass]@]host[:port]
        if '@' in parsed.netloc:
            userinfo, hostinfo = parsed.netloc.rsplit('@', 1)
            if ':' in userinfo:
                u, p = userinfo.split(':', 1)
                username = unquote(u)
                password = unquote(p)
            else:
                username = unquote(userinfo)
        else:
            hostinfo = parsed.netloc
        if ':' in hostinfo:
            h, p = hostinfo.split(':', 1)
            host = h
            try:
                port = int(p)
            except Exception:
                port = None
        else:
            host = hostinfo

    # path may start with /database
    database = parsed.path[1:] if parsed.path and parsed.path.startswith(
        '/') else (parsed.path or '')

    # parse query string into simple dict (take first value)
    raw_qs = parse_qs(parsed.query)
    params = {k: v[0] for k, v in raw_qs.items()}

    # Capture schema/search_path if present so the UI can show it separately
    schema_val = None
    if 'currentSchema' in params and params.get('currentSchema'):
        schema_val = params.get('currentSchema')
    elif 'search_path' in params and params.get('search_path'):
        schema_val = params.get('search_path')

    # Map common JDBC params to psycopg2/sqlalchemy equivalents
    # ssl=false -> sslmode=disable
    if params.get('ssl', '').lower() == 'false':
        params['sslmode'] = 'disable'
    # If a currentSchema was provided, also set it as an options parameter so libpq picks it up
    if 'currentSchema' in params and params.get('currentSchema'):
        params.setdefault(
            'options', f"-c search_path={params['currentSchema']}")
    # characterEncoding usually indicates client encoding; we assume UTF-8

    # Handle timezone-like JDBC params: TimeZone, serverTimezone, timezone
    tz_value = None
    for key in list(params.keys()):
        if key.lower() in ("timezone", "servertimezone"):
            tz_value = params.pop(key)
            break
    if tz_value:
        # append timezone setting to options (-c TimeZone=...)
        existing_opts = params.get('options')
        tz_opt = f"-c TimeZone={tz_value}"
        if existing_opts:
            # ensure spacing
            params['options'] = existing_opts + ' ' + tz_opt
        else:
            params['options'] = tz_opt

    # Remove JDBC-only parameters so they are not passed verbatim to libpq/psycopg2
    for jkey in ('currentSchema', 'ssl', 'characterEncoding', 'TimeZone', 'serverTimezone'):
        params.pop(jkey, None)

    return {
        'conn_type': conn_type,
        'host': host,
        'port': port,
        'database': database,
        'username': username,
        'password': password,
        'params': params,
        'schema': schema_val,
    }


class ConnectionManager:
    """Manage DB connections (engines) and persist connection configs."""

    def __init__(self, config_path: Path | None = None):
        self._engines: Dict[str, Engine] = {}
        self._configs: Dict[str, Dict[str, Any]] = {}
        self.config_path = Path(
            config_path) if config_path else DEFAULT_CONFIG_PATH
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self._load_config()

    def _load_config(self) -> None:
        if not self.config_path.exists():
            return
        # Try multiple encodings because users may have config written in other encodings on Windows
        encodings_to_try = ("utf-8", "utf-8-sig", "cp936", "latin-1")
        raw = None
        for enc in encodings_to_try:
            try:
                with open(self.config_path, "r", encoding=enc) as f:
                    raw = f.read()
                # if read succeeded, try to parse JSON
                data = json.loads(raw)
                break
            except (UnicodeDecodeError, json.JSONDecodeError):
                raw = None
                continue
            except Exception:
                # other IO errors: give up loading
                raw = None
                break

        if raw is None:
            # backup the problematic config file and continue with empty configs
            try:
                bad_path = self.config_path.with_suffix(
                    self.config_path.suffix + ".bak")
                with open(self.config_path, "rb") as fsrc, open(bad_path, "wb") as fdst:
                    fdst.write(fsrc.read())
            except Exception:
                pass
            return

        # data is parsed JSON here
        try:
            # Do NOT attempt to create engines or test connecting at load time. Just keep configs
            # and create engines lazily in get_connection. This avoids network calls and unexpected
            # credential validation during application startup (per user request).
            for name, cfg in data.items():
                # Decode password saved with rot13 (best-effort). Keep other fields as-is.
                try:
                    if isinstance(cfg, dict) and isinstance(cfg.get('password'), str):
                        try:
                            decoded = codecs.decode(
                                cfg.get('password'), 'rot_13')
                            cfg['password'] = decoded
                        except Exception:
                            # if decoding fails, keep original value
                            pass
                except Exception:
                    pass
                # preserve config as-is; do not create or test engine now
                self._configs[name] = cfg
        except Exception:
            # protect from unexpected structure
            return

        # Log the loaded config names for debugging (do not log secrets)
        try:
            logger.debug("Loaded connection configs: %r",
                         list(self._configs.keys()))
        except Exception:
            pass

    def _save_config(self) -> None:
        try:
            # write all known configs
            # Encode passwords using rot13 for simple obfuscation before writing
            to_write: Dict[str, Any] = {}
            for name, cfg in self._configs.items():
                try:
                    if isinstance(cfg, dict):
                        cfg_copy = dict(cfg)
                        if isinstance(cfg_copy.get('password'), str):
                            try:
                                cfg_copy['password'] = codecs.encode(
                                    cfg_copy['password'], 'rot_13')
                            except Exception:
                                # if encoding fails, keep original
                                pass
                        to_write[name] = cfg_copy
                    else:
                        to_write[name] = cfg
                except Exception:
                    to_write[name] = cfg

            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(to_write, f, indent=2)
        except Exception:
            # best-effort; ignore errors
            pass

    def _log_engine_url(self, name: str, engine: Engine) -> None:
        """Log the engine's connection URL with password hidden for diagnostics."""
        try:
            url_obj = getattr(engine, 'url', None)
            if url_obj is None:
                # Fallback when engine has no url attribute
                safe = '<engine-without-url>'
            else:
                try:
                    # SQLAlchemy URL can render without password
                    safe = url_obj.render_as_string(hide_password=True)
                except Exception:
                    safe = str(url_obj)
            logger.debug("Engine for %s created: %s", name, safe)
        except Exception:
            # Never raise from logging helper
            pass

    def add_sqlite_connection(self, path: str) -> str:
        """Add a SQLite connection by file path. Returns a connection name.

        This function will register the connection without forcing a live connect/test so the
        application remains responsive and does not attempt credential validation unexpectedly.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"SQLite file not found: {path}")
        name = f"SQLite: {os.path.basename(path)}"
        if name in self._engines:
            idx = 1
            base = name
            while name in self._engines:
                name = f"{base} ({idx})"
                idx += 1
        url = f"sqlite:///{os.path.abspath(path)}"
        engine = create_engine(url, future=True)
        # Do not perform an immediate test connect here per 'do not unittest' preference.
        self._engines[name] = engine
        # Persist only the page inputs; do NOT save the full URL string.
        self._configs[name] = {"type": "sqlite", "path": os.path.abspath(path)}
        self._save_config()
        return name

    def add_connection(self, name: str, conn_type: str, **kwargs) -> str:
        """Add a generic connection. conn_type: 'postgresql'|'mysql'. kwargs expected: host, port, user, password, database, driver(optional).

        Returns the connection name.
        """
        # Normalize common aliases so callers can pass either 'username' or 'user', 'pwd' or 'password'
        if 'username' in kwargs and 'user' not in kwargs:
            kwargs['user'] = kwargs.pop('username')
        if 'pwd' in kwargs and 'password' not in kwargs:
            kwargs['password'] = kwargs.pop('pwd')

        # If caller passed a JDBC URL via kwargs['jdbc'], parse it and populate kwargs
        jdbc_raw = kwargs.pop('jdbc', None)
        if jdbc_raw:
            try:
                parsed = parse_jdbc_url(jdbc_raw)
                # override provided conn_type with detected type
                conn_type = parsed.get('conn_type', conn_type)
                # populate fields if not already provided
                kwargs.setdefault('host', parsed.get('host'))
                kwargs.setdefault('port', parsed.get('port'))
                kwargs.setdefault('database', parsed.get('database'))
                if parsed.get('username') is not None:
                    kwargs.setdefault('user', parsed.get('username'))
                if parsed.get('password') is not None:
                    kwargs.setdefault('password', parsed.get('password'))
                # merge params into kwargs (e.g., sslmode, options)
                for k, v in parsed.get('params', {}).items():
                    # avoid overwriting explicit kwargs
                    kwargs.setdefault(k, v)
                # also preserve schema if provided by JDBC URL
                if parsed.get('schema'):
                    kwargs.setdefault('schema', parsed.get('schema'))
            except Exception as e:
                raise RuntimeError(f"Failed to parse JDBC URL: {e}") from e

        # Helper to coerce bytes/unicode inputs to str robustly
        def _to_str_with_fallback(v):
            if v is None:
                return None
            if isinstance(v, str):
                return v
            if isinstance(v, bytes):
                for enc in ("utf-8", "utf-8-sig", "cp936", "latin-1"):
                    try:
                        return v.decode(enc)
                    except Exception:
                        continue
                # last resort
                return v.decode("utf-8", errors="replace")
            try:
                return str(v)
            except Exception:
                return None

        # prefer 'user' key but accept normalized value from above
        user = kwargs.get("user", "")
        password = kwargs.get("password", "")
        host = kwargs.get("host", "localhost")
        port = kwargs.get("port", None)
        db = kwargs.get("database", "")

        # coerce inputs to safe str to avoid UnicodeDecodeError from drivers expecting text
        user = _to_str_with_fallback(user) or None
        password = _to_str_with_fallback(password) or None
        host = _to_str_with_fallback(host) or None
        db = _to_str_with_fallback(db) or None

        if conn_type == "postgresql":
            driver = kwargs.get("driver", "psycopg2")
            drivername = f"postgresql+{driver}"
            # port default
            if port is None:
                port = 5432
        elif conn_type == "mysql":
            driver = kwargs.get("driver", "pymysql")
            drivername = f"mysql+{driver}"
            if port is None:
                port = 3306
        else:
            raise ValueError(f"Unsupported connection type: {conn_type}")

        # Use SQLAlchemy URL.create to properly quote username/password and build URL object
        try:
            # collect extra kwargs as query parameters (e.g., sslmode, options)
            query_params = {}
            for k, v in kwargs.items():
                if k in ("host", "port", "user", "password", "database", "driver", "jdbc"):
                    continue
                if v is None:
                    continue
                query_params[k] = str(v)

            # If the caller provided a schema/search_path for PostgreSQL, ensure it's applied via
            # the libpq 'options' parameter ("-c search_path=...") so the session default schema is set.
            if conn_type == "postgresql":
                # Determine schema from explicit kwargs first, then from any parsed query params, then
                # from options if present. We persist the canonical schema separately in the saved
                # configuration so the UI/metadata fetcher can rely on it instead of parsing options.
                schema_val = None
                for schema_key in ("schema", "search_path", "currentSchema"):
                    if kwargs.get(schema_key):
                        schema_val = str(kwargs.get(schema_key))
                        break
                # If not in kwargs, check query_params (e.g., if JDBC parsing populated them)
                if not schema_val:
                    for schema_key in ("schema", "search_path", "currentSchema"):
                        if query_params.get(schema_key):
                            schema_val = str(query_params.get(schema_key))
                            break
                # Finally, try to detect schema inside an existing 'options' value like
                # "-c search_path=foo -c TimeZone=..."
                if not schema_val and query_params.get('options'):
                    import re

                    m = re.search(
                        r"search_path\s*=\s*([\w\",]+)", query_params.get('options'))
                    if m:
                        schema_val = m.group(1).strip().strip('"')

                # If we found a schema, ensure options contains -c search_path=... so libpq picks it up
                if schema_val:
                    existing_opts = query_params.get('options')
                    schema_opt = f"-c search_path={schema_val}"
                    query_params['options'] = (
                        existing_opts + ' ' + schema_opt) if existing_opts else schema_opt

                # Important: remove any schema-like keys from the query parameters so they are not
                # passed verbatim to libpq/psycopg2 which rejects unknown dsn options like 'schema'.
                for _k in ('schema', 'search_path', 'currentSchema'):
                    query_params.pop(_k, None)

            url_obj = URL.create(
                drivername=drivername,
                username=user or None,
                password=password or None,
                host=host or None,
                port=int(port) if port else None,
                database=db or None,
                query=query_params or None,
            )
        except Exception as e:
            raise RuntimeError(f"Invalid connection parameters: {e}") from e

        # ensure unique display name
        display_name = name
        if display_name in self._engines or display_name in self._configs:
            idx = 1
            base = display_name
            while display_name in self._engines or display_name in self._configs:
                display_name = f"{base} ({idx})"
                idx += 1

        engine = create_engine(url_obj, future=True)

        # If schema was provided for PostgreSQL, also attach a connect-time listener to ensure
        # the search_path is set on DBAPI connections (covers drivers/hosts that ignore 'options').
        try:
            if conn_type == "postgresql":
                schema_to_apply = None
                for schema_key in ("schema", "search_path", "currentSchema"):
                    if schema_key in kwargs and kwargs.get(schema_key):
                        schema_to_apply = str(kwargs.get(schema_key))
                        break
                if schema_to_apply:
                    from sqlalchemy import event

                    def _set_search_path(dbapi_conn, connection_record):
                        try:
                            cur = dbapi_conn.cursor()
                            # Use quoted identifier only if simple identifier; keep simple to avoid SQL injection
                            # quote the schema value to produce a safe literal (e.g. 'schema')
                            cur.execute(
                                f"SET search_path TO {schema_to_apply!r}")
                        except Exception:
                            # best-effort; do not raise here
                            pass

                    event.listen(engine, "connect", _set_search_path)
        except Exception:
            # keep best-effort: do not fail connection creation if listener attachment fails
            pass

        # Do not perform an immediate test connect here. Engines are created but connections
        # will be validated when first used (get_connection). This keeps the UI responsive
        # and avoids performing credential validation at add time.

        self._engines[display_name] = engine
        # Log a sanitized connection URL for diagnostics (password hidden)
        try:
            self._log_engine_url(display_name, engine)
        except Exception:
            pass
        # store config values; by default redact password. If save_password was requested and
        # a password was provided, persist it (insecure but requested). Storing plaintext passwords is
        # insecure; prefer system keyring instead for production. Persist the schema explicitly
        # so downstream metadata fetchers don't need to parse it out of 'options'.
        cfg_vals = {k: v for k, v in kwargs.items() if k in (
            "host", "port", "user", "database", "schema")}
        # ensure canonical keys
        cfg_vals["user"] = user
        # Persist password directly as requested (insecure but requested by user)
        cfg_vals["password"] = password

        # Ensure schema is persisted reliably by computing a final_schema from multiple sources
        final_schema = None
        try:
            # priority: explicit kwargs, earlier-detected schema_val (from above), query_params, options parsing
            if kwargs.get('schema'):
                final_schema = str(kwargs.get('schema'))
            elif 'schema_val' in locals() and schema_val:
                final_schema = str(schema_val)
            else:
                for schema_key in ('schema', 'search_path', 'currentSchema'):
                    if query_params.get(schema_key):
                        final_schema = str(query_params.get(schema_key))
                        break
                if not final_schema and query_params.get('options'):
                    import re

                    m = re.search(
                        r"search_path\s*=\s*([\w\",]+)", query_params.get('options'))
                    if m:
                        final_schema = m.group(1).strip().strip('"')
        except Exception:
            final_schema = None

        if final_schema:
            cfg_vals['schema'] = final_schema

        # persist any query params (sslmode, options, etc.) for reproducibility
        cfg_params = {k: v for k, v in query_params.items(
        )} if 'query_params' in locals() else {}
        # Persist only page inputs and params; do not save the full URL string.
        # Keep driver so we can reconstruct the URL on load.
        cfg_record = {"type": conn_type, "driver": driver,
                      "params": cfg_params, **cfg_vals}
        self._configs[display_name] = cfg_record
        self._save_config()
        return display_name

    def get_connection(self, name: str) -> Engine:
        """Return the SQLAlchemy Engine for the given connection name."""
        if name not in self._engines:
            # try to reconstruct from config; provide robust fallback when stored passwords
            # may have been rot13-encoded or stored in different forms. Log exceptions so
            # callers can diagnose why an engine wasn't created (without printing secrets).
            if name in self._configs:
                cfg = self._configs[name]
                last_exc = None
                try:
                    engine = None
                    # Build a list of candidate passwords to try: stored value and a rot13-decoded
                    # variant. Many configs historically stored passwords using rot13 obfuscation.
                    import codecs as _codecs

                    raw_pw = cfg.get('password') if isinstance(
                        cfg.get('password'), (str, bytes)) else None
                    pw_candidates = [raw_pw]
                    if isinstance(raw_pw, str):
                        try:
                            alt = _codecs.decode(raw_pw, 'rot_13')
                            if alt != raw_pw:
                                pw_candidates.append(alt)
                        except Exception:
                            pass

                    # Try each password candidate until one succeeds
                    for try_pw in pw_candidates:
                        try:
                            # Reconstruct engine from saved fields. Backwards-compat: if a legacy 'url'
                            # exists in the config, use it directly.
                            engine = None
                            if isinstance(cfg.get('url'), str):
                                engine = create_engine(cfg['url'], future=True)
                            else:
                                ctype = cfg.get('type')
                                if ctype == 'sqlite':
                                    path = cfg.get('path')
                                    if not path:
                                        raise RuntimeError(
                                            'Missing sqlite path in config')
                                    url = f"sqlite:///{os.path.abspath(path)}"
                                    engine = create_engine(url, future=True)
                                else:
                                    drv = cfg.get('driver') or (
                                        'psycopg2' if ctype == 'postgresql' else 'pymysql')
                                    if ctype == 'postgresql':
                                        drivername = f"postgresql+{drv}"
                                    elif ctype == 'mysql':
                                        drivername = f"mysql+{drv}"
                                    else:
                                        raise ValueError(
                                            f"Unsupported connection type in config: {ctype}")

                                    user = cfg.get('user') or None
                                    password = try_pw or None
                                    host = cfg.get('host') or None
                                    port = int(cfg['port']) if cfg.get(
                                        'port') else None
                                    database = cfg.get(
                                        'database') or cfg.get('db') or None
                                    # Sanitize stored params: do not pass a top-level 'schema' or
                                    # JDBC-like schema keys directly as libpq connection options.
                                    # Instead map them into an 'options' value so psycopg2/libpq
                                    # receives -c search_path=... which is valid.
                                    raw_params = cfg.get('params') if isinstance(
                                        cfg.get('params'), dict) else {}
                                    query = dict(
                                        raw_params) if raw_params else {}

                                    # Extract schema from possible places: explicit top-level key or params
                                    schema_val = None
                                    if cfg.get('schema'):
                                        schema_val = cfg.get('schema')
                                    else:
                                        for k in ('schema', 'search_path', 'currentSchema'):
                                            if k in query:
                                                schema_val = query.pop(k)
                                                break

                                    # If a schema was found, ensure it's passed via 'options' as -c search_path=...
                                    if schema_val:
                                        existing_opts = query.get('options')
                                        schema_opt = f"-c search_path={schema_val}"
                                        query['options'] = (
                                            existing_opts + ' ' + schema_opt) if existing_opts else schema_opt

                                    # Remove any accidental 'schema' key left in params to avoid invalid dsn
                                    query.pop('schema', None)

                                    url_obj = URL.create(
                                        drivername=drivername,
                                        username=user,
                                        password=password,
                                        host=host,
                                        port=port,
                                        database=database,
                                        query=query or None,
                                    )
                                    engine = create_engine(url_obj, future=True)

                            # If a schema was saved for a PostgreSQL connection, attach a connect listener
                            # so the reconstructed engine applies the saved search_path to sessions.
                            try:
                                if cfg.get('type') == 'postgresql' and engine is not None:
                                    # Prefer explicit 'schema' key if present
                                    schema_to_apply = cfg.get('schema')

                                    # If not present, try to detect from stored params (e.g. options='-c search_path=...')
                                    if not schema_to_apply and isinstance(cfg.get('params'), dict):
                                        params = cfg.get('params', {})
                                        for k in ('schema', 'search_path', 'currentSchema'):
                                            if params.get(k):
                                                schema_to_apply = params.get(k)
                                                break
                                        if not schema_to_apply and params.get('options'):
                                            import re as _re

                                            opts = params.get('options')
                                            m = _re.search(
                                                r"search_path\s*=\s*([\w\",]+)", opts)
                                            if m:
                                                schema_to_apply = m.group(
                                                    1).strip().strip('"')

                                    if schema_to_apply and engine is not None:
                                        from sqlalchemy import event

                                        def _set_search_path(dbapi_conn, connection_record):
                                            try:
                                                cur = dbapi_conn.cursor()
                                                cur.execute(
                                                    f"SET search_path TO {schema_to_apply!r}")
                                            except Exception:
                                                pass

                                        event.listen(
                                            engine, 'connect', _set_search_path)
                            except Exception:
                                # best-effort; do not fail engine reconstruction for metadata-only configs
                                pass

                            # Perform the quick connect attempt in a separate thread with a timeout so
                            # slow network/driver behavior cannot block the caller indefinitely.
                            import threading

                            connect_result = {'ok': False, 'error': None}

                            def _try_connect():
                                try:
                                    with engine.connect() as conn:
                                        pass
                                    connect_result['ok'] = True
                                except Exception as e:
                                    connect_result['error'] = e

                            thr = threading.Thread(target=_try_connect, daemon=True)
                            thr.start()
                            thr.join(5)  # short timeout for responsiveness
                            if not connect_result['ok']:
                                last_exc = connect_result['error'] or RuntimeError(
                                    f"Connection test timed out after 5s for '{name}'")
                                try:
                                    engine.dispose()
                                except Exception:
                                    pass
                                engine = None
                                logger.debug(
                                    "Connection test failed or timed out for '%s' with a password candidate: %s", name, last_exc)
                                continue

                            # success
                            self._engines[name] = engine
                            # Log a sanitized connection URL for diagnostics (password hidden)
                            try:
                                self._log_engine_url(name, engine)
                            except Exception:
                                pass
                            engine = None
                            break
                        except Exception as e:
                            last_exc = e
                            logger.debug(
                                "Engine construction attempt failed for '%s': %s", name, e, exc_info=True)
                            try:
                                if engine is not None:
                                    engine.dispose()
                            except Exception:
                                pass
                            engine = None
                            continue

                    if name not in self._engines:
                        logger.debug(
                            "All engine reconstruction attempts failed for '%s'; last error: %r", name, last_exc)
                except Exception:
                    # keep best-effort: do not fail engine reconstruction for metadata-only configs
                    pass
        # If after reconstruction there is still no engine, raise a clear error so callers can
        # handle the missing-engine case instead of getting a KeyError.
        if name not in self._engines:
            raise RuntimeError(
                f"Connection '{name}' is not available (engine was not created)")
        return self._engines[name]

    def list_connections(self) -> list:
        # merge keys from configs and engines to preserve configs without live engine
        names = set(self._configs.keys()) | set(self._engines.keys())
        names_list = sorted(names)
        try:
            logger.debug("Listing connections: engines=%r, configs=%r, merged=%r", list(
                self._engines.keys()), list(self._configs.keys()), names_list)
        except Exception:
            pass
        return names_list

    def remove_connection(self, name: str) -> None:
        if name in self._engines:
            try:
                self._engines[name].dispose()
            except Exception:
                pass
            del self._engines[name]
        if name in self._configs:
            del self._configs[name]
            self._save_config()
