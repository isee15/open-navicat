import json
import logging
import os
from typing import Optional
import requests
import codecs
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .settings import load_ai_settings

logger = logging.getLogger(__name__)


class AIClientError(RuntimeError):
    pass


def _build_session(retries: int = 3, backoff_factor: float = 0.5, status_forcelist=(500, 502, 503, 504)) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist, allowed_methods=frozenset(["GET", "POST"]))
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def generate_sql_from_nl(nl: str, timeout: int = 15, max_tokens: int = 1024, stream_callback: Optional[callable] = None, stop_event: Optional[threading.Event] = None) -> str:
    """Use an OpenAI-compatible chat/completions API to generate SQL from natural language.

    Behavior:
      - Loads settings via load_ai_settings()
      - Composes a chat-style payload {model, messages: [{role:'user', content: prompt}], temperature:0}
      - Posts to base_url; if base_url does not appear to contain '/chat' or '/completions', append '/chat/completions'.
      - Expects a response with choices[0].message.content (OpenAI Chat Completions).

    Raises AIClientError on network or parsing errors.
    """
    if not nl or not nl.strip():
        raise AIClientError("Empty prompt")

    settings = load_ai_settings()
    base_url = settings.get("base_url") or settings.get("api_url")
    model = settings.get("model_name")

    # Simplified: stored value may be an env var name or the actual key.
    api_key_setting = settings.get("api_key") or settings.get("raw_api_key") or ""
    api_key = os.environ.get(api_key_setting, api_key_setting) if api_key_setting else ""

    # Mask preview for logging (do not print secrets)
    key_preview = (api_key[:5] + '...') if api_key else '<none>'
    logger.debug("AI config: resolved_api_key_preview=%s", key_preview)

    if not base_url:
        raise AIClientError("AI base URL not configured")

    # Attempt to include current DB schema in the prompt if enabled in settings.
    schema_text = ""
    include_schema = settings.get("include_schema_in_prompt", True)
    if include_schema:
        try:
            # Lazy import to avoid hard dependency; projects can provide a helper
            # function get_current_db_schema() that returns a string representation
            # of the current connection's schema (tables/columns/primary keys/etc.).
            get_current_db_schema = None
            # Try several import strategies: prefer absolute imports so running as a script (with src on sys.path)
            # works; fall back to package-relative imports when used as a package.
            try:
                logger.debug("Attempting absolute import db.metadata.get_current_db_schema")
                from db.metadata import get_current_db_schema  # type: ignore
                logger.debug("Imported db.metadata.get_current_db_schema successfully (absolute)")
            except Exception as e1:
                logger.debug("Absolute import db.metadata.get_current_db_schema failed: %s", e1)
                try:
                    logger.debug("Attempting package-relative import ..db.metadata.get_current_db_schema")
                    from ..db.metadata import get_current_db_schema  # type: ignore
                    logger.debug("Imported ..db.metadata.get_current_db_schema successfully (relative)")
                except Exception as e2:
                    logger.debug("Relative import ..db.metadata.get_current_db_schema failed: %s", e2)
                    get_current_db_schema = None  # type: ignore

            if not callable(get_current_db_schema):
                logger.debug("get_current_db_schema not available or not callable; skipping schema inclusion")
            else:
                try:
                    logger.debug("Calling get_current_db_schema() to fetch schema for prompt")
                    fetched = get_current_db_schema()
                    logger.debug("get_current_db_schema() returned type=%s", type(fetched))
                    if fetched:
                        schema_text = fetched if isinstance(fetched, str) else str(fetched)
                        # Do not impose an artificial character limit on the fetched schema.
                        # Log the length for observability; include the full schema_text in prompts.
                        try:
                            logger.debug("Fetched DB schema length=%d; including full schema in prompt", len(schema_text))
                        except Exception:
                            pass
                        # Log a short preview for debugging (avoid printing secrets)
                        try:
                            preview = schema_text[:500]
                            logger.debug("Fetched DB schema preview (first 500 chars): %s", preview.replace('\n', '\\n'))
                        except Exception:
                            pass
                    else:
                        logger.debug("get_current_db_schema() returned empty or None; no schema will be included in prompt")
                except Exception as e:
                    logger.debug("Failed to fetch DB schema: %s", e, exc_info=True)
        except Exception:
            # Any unexpected errors must not break SQL generation
            logger.debug("Error while attempting to include DB schema in prompt", exc_info=True)

    # prefer an endpoint that ends with /chat/completions; otherwise append it
    url = base_url.rstrip('/')
    if '/chat' not in url and '/completions' not in url:
        url = url + '/chat/completions'

    if include_schema:
        if schema_text:
            logger.debug("Including DB schema in prompt (length=%d)", len(schema_text))
        else:
            logger.debug("include_schema_in_prompt is true but no schema_text was found; prompt will omit schema")

    session = _build_session()
    headers = {"Content-Type": "application/json"}
    # log a short preview (no full keys) to aid debugging
    key_preview = (api_key[:5] + '...') if api_key else '<none>'
    logger.debug("AI request to %s api_key_setting=%s resolved_api_key_preview=%s", url, api_key_setting or '<none>', key_preview)
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        
    # Construct a prompt instructing model to return only SQL
    base_prompt = (
        "You are a helpful assistant that converts a developer's natural language request into a single valid SQL query. "
        "Return only the SQL statement, do not wrap it in markdown or explain it. If the request is ambiguous, return a commented SQL with a short clarifying comment. 根据用户输入使用中文或者英文.\n\n"
    )

    # If we were able to fetch schema_text, prepend it to provide context
    if schema_text:
        prompt = (
            f"Current DB schema:\n```{schema_text}```\n\n"
            f"{base_prompt}UserInput: ```{nl}```\n\nSQL:"
        )
    else:
        prompt = (
            f"{base_prompt}UserInput: ```{nl}```\n\nSQL:"
        )
    logger.debug("Final prompt sent to AI: %s", prompt)
    payload = {
        "model": model or "",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": max_tokens,
    }
    # if caller provided a stream_callback, request streaming from the server if supported
    want_stream = callable(stream_callback)
    if want_stream:
        payload["stream"] = True

    try:
        # Use streaming mode when requested and supported
        if want_stream:
            try:
                r = session.post(url, headers=headers, data=json.dumps(payload), timeout=timeout, stream=True)
            except Exception:
                # Fall back to non-streaming call
                r = session.post(url, headers=headers, data=json.dumps({k: v for k, v in payload.items() if k != 'stream'}), timeout=timeout)

            try:
                r.raise_for_status()
            except Exception as e:
                body = None
                try:
                    body = r.text
                except Exception:
                    body = None
                raise AIClientError(f"HTTP error from AI endpoint: {e}; body={body}; api_key_setting={api_key_setting or '<none>'}; api_key_preview={key_preview}") from e

            # ensure requests will decode bytes as utf-8 when asked to decode
            try:
                r.encoding = 'utf-8'
            except Exception:
                pass

            full = []
            try:
                decoder = codecs.getincrementaldecoder('utf-8')()
                buf = ''
                # iterate over raw bytes to handle multi-byte characters crossing chunk boundaries
                for chunk_bytes in r.iter_content(chunk_size=1024):
                    if chunk_bytes is None:
                        continue
                    # cooperative cancellation: stop if requested
                    try:
                        if stop_event is not None and stop_event.is_set():
                            # abort streaming early
                            raise StopIteration
                    except Exception:
                        # ignore problems checking stop_event
                        pass
                    try:
                        text_chunk = decoder.decode(chunk_bytes)
                    except Exception:
                        # fallback: try a best-effort decode
                        try:
                            text_chunk = chunk_bytes.decode('utf-8', errors='replace')
                        except Exception:
                            text_chunk = ''
                    if not text_chunk:
                        continue
                    buf += text_chunk
                    # split into lines; SSE events are line-delimited
                    while '\n' in buf:
                        line, buf = buf.split('\n', 1)
                        line = line.strip()
                        if not line:
                            continue
                        # Normalize payload: support both 'data: {...}' SSE lines and plain JSON lines
                        # logger.debug("Received chunk line: %s", line)
                        if line.startswith('data:'):
                            payload_str = line[len('data:'):].strip()
                        else:
                            payload_str = line
                        if payload_str == '[DONE]':
                            # consume remaining and break
                            buf = ''
                            raise StopIteration

                        parts = [payload_str]
                        if payload_str.startswith('{') and '}{' in payload_str:
                            parts = payload_str.replace('}{', '}\n{').splitlines()

                        for part in parts:
                            part = part.strip()
                            if not part:
                                continue
                            try:
                                obj = json.loads(part)
                            except Exception:
                                # forward a preview raw string in a structured form so the UI can log it
                                try:
                                    if callable(stream_callback):
                                        stream_callback(("preview", part))
                                    full.append(part)
                                except Exception:
                                    pass
                                continue

                            # If this chunk includes usage information, surface it to the UI
                            try:
                                if isinstance(obj, dict) and obj.get('usage'):
                                    try:
                                        if callable(stream_callback):
                                            stream_callback(("usage", obj.get('usage')))
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                            # Some SSE implementations (or non-SSE chunking variants) signal stream end
                            # by including finish_reason:'stop' on a choice. Handle that case explicitly
                            # so we stop streaming even when the chunk's delta.content is empty.
                            try:
                                if isinstance(obj, dict):
                                    choices_fr = obj.get('choices')
                                    if isinstance(choices_fr, list):
                                        for ch_fr in choices_fr:
                                            try:
                                                if isinstance(ch_fr, dict) and ch_fr.get('finish_reason') == 'stop':
                                                    # consume remaining and break
                                                    buf = ''
                                                    raise StopIteration
                                            except StopIteration:
                                                raise
                                            except Exception:
                                                # ignore and continue checking other choices
                                                pass
                            except StopIteration:
                                raise
                            except Exception:
                                pass

                            # Extract incremental content in a best-effort, human-friendly way.
                            chunk_pieces = []
                            try:
                                choices = obj.get('choices') if isinstance(obj, dict) else None
                                if isinstance(choices, list) and len(choices) > 0:
                                    for ch in choices:
                                        delta = ch.get('delta') or {}
                                        # reasoning_content is often streamed for chain-of-thought
                                        rc = delta.get('reasoning_content') or delta.get('reasoning') or None
                                        if rc:
                                            chunk_pieces.append(("reasoning", rc))
                                        c = delta.get('content') or delta.get('text') or None
                                        if c:
                                            chunk_pieces.append(("content", c))
                                        if not chunk_pieces:
                                            msg = ch.get('message') or {}
                                            if isinstance(msg, dict):
                                                msg_cont = msg.get('content') or msg.get('text')
                                                if msg_cont:
                                                    chunk_pieces.append(("content", msg_cont))
                            except Exception:
                                pass

                            if chunk_pieces:
                                # chunk_pieces may contain tuples (kind, text); emit each separately
                                for piece in chunk_pieces:
                                    try:
                                        if isinstance(piece, tuple) and len(piece) == 2:
                                            kind, txt = piece
                                            if callable(stream_callback):
                                                stream_callback((kind, str(txt)))
                                            full.append(str(txt))
                                        else:
                                            # fallback to plain text
                                            if callable(stream_callback):
                                                stream_callback(("content", str(piece)))
                                            full.append(str(piece))
                                    except Exception:
                                        pass
                            else:
                                try:
                                    if isinstance(obj, dict) and obj.get('message') and isinstance(obj.get('message'), dict):
                                        mc = obj.get('message').get('content') or obj.get('message').get('text')
                                        if mc:
                                            if callable(stream_callback):
                                                stream_callback(("content", mc))
                                            full.append(mc)
                                            continue
                                except Exception:
                                    pass
                                try:
                                    preview = json.dumps({k: v for k, v in obj.items() if k in ('choices', 'object', 'id')})
                                    if callable(stream_callback):
                                        stream_callback(("preview", preview))
                                    full.append(preview)
                                except Exception:
                                    pass
                # flush any remaining decoded text
                try:
                    rem = decoder.decode(b'', final=True)
                    if rem:
                        buf += rem
                except Exception:
                    pass
                # process any leftover buffer as a final line
                if buf:
                    line = buf.strip()
                    if line and line != '[DONE]':
                        try:
                            obj = json.loads(line)
                        except Exception:
                            try:
                                stream_callback(line)
                                full.append(line)
                            except Exception:
                                pass
                        else:
                            # If the final JSON includes usage, surface it as well
                            try:
                                if isinstance(obj, dict) and obj.get('usage'):
                                    try:
                                        if callable(stream_callback):
                                            stream_callback(("usage", obj.get('usage')))
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                            try:
                                choices = obj.get('choices') if isinstance(obj, dict) else None
                                if isinstance(choices, list) and len(choices) > 0:
                                    for ch in choices:
                                        delta = ch.get('delta') or {}
                                        rc = delta.get('reasoning_content') or delta.get('reasoning') or None
                                        if rc:
                                            if callable(stream_callback):
                                                stream_callback(("reasoning", rc))
                                            full.append(rc)
                                        c = delta.get('content') or delta.get('text') or None
                                        if c:
                                            if callable(stream_callback):
                                                stream_callback(("content", c))
                                            full.append(c)
                            except Exception:
                                pass
                combined = ''.join(full)
            except StopIteration:
                combined = ''.join(full)
            except Exception:
                combined = None

            if combined is not None:
                text = combined
            else:
                # streaming failed; fall back to blocking call
                r2 = session.post(url, headers=headers, data=json.dumps({k: v for k, v in payload.items() if k != 'stream'}), timeout=timeout)
                try:
                    r2.raise_for_status()
                except Exception as e:
                    body = None
                    try:
                        body = r2.text
                    except Exception:
                        body = None
                    raise AIClientError(f"HTTP error from AI endpoint: {e}; body={body}") from e
                try:
                    j = r2.json()
                except ValueError:
                    text = r2.text or ''
                else:
                    # Surface usage info from the non-streaming fallback response if present
                    try:
                        if isinstance(j, dict) and j.get('usage'):
                            try:
                                if callable(stream_callback):
                                    stream_callback(("usage", j.get('usage')))
                            except Exception:
                                pass
                    except Exception:
                        pass
                    # reuse non-stream parsing logic below
                    # fall through to set text variable via j
                    text = None

        else:
            # non-streaming simple request
            r = session.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
            try:
                r.raise_for_status()
            except Exception as e:
                body = None
                try:
                    body = r.text
                except Exception:
                    body = None
                raise AIClientError(f"HTTP error from AI endpoint: {e}; body={body}; api_key_setting={api_key_setting or '<none>'}; api_key_preview={key_preview}") from e

            try:
                j = r.json()
            except ValueError:
                raise AIClientError("AI response is not valid JSON")

            # parse JSON OpenAI-style response
            text = None
            # Surface usage info for non-streaming responses as a preview
            try:
                if isinstance(j, dict) and j.get('usage'):
                    try:
                        if callable(stream_callback):
                            stream_callback(("usage", j.get('usage')))
                    except Exception:
                        pass
            except Exception:
                pass
            if isinstance(j, dict):
                choices = j.get("choices")
                if isinstance(choices, list) and len(choices) > 0:
                    first = choices[0]
                    # Support both chat (message.content) and legacy completions (text)
                    if isinstance(first.get("message"), dict) and first.get("message").get("content"):
                        text = first.get("message").get("content")
                    elif first.get("text"):
                        text = first.get("text")

            if not text:
                # fallback to other possible keys
                if isinstance(j, dict) and j.get("result"):
                    text = j.get("result")
                elif isinstance(j, dict) and j.get("data"):
                    d = j.get("data")
                    if isinstance(d, str):
                        text = d
                    elif isinstance(d, dict) and d.get("text"):
                        text = d

            if not text:
                # last resort: raw response body
                text = r.text or ""

        # final cleanup: ensure text is string and strip markdown fences
        text = (text or "").strip()
        if text.startswith("```") and text.endswith("```"):
            parts = text.splitlines()
            if parts:
                parts = parts[1:]
            if parts and parts[-1].strip().startswith("```"):
                parts = parts[:-1]
            text = "\n".join(parts).strip()

        return text
    except AIClientError:
        raise
    except Exception as e:
        logger.exception("AI client failed")
        raise AIClientError(str(e)) from e
