import os
import json
from pathlib import Path
from typing import Dict, Any

CONFIG_DIR = Path(os.path.expanduser("~")) / ".catdbviewer"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
AI_SETTINGS_PATH = CONFIG_DIR / "ai_settings.json"


def load_ai_settings() -> Dict[str, Any]:
    """Load AI settings from ai_settings.json.

    Simplified behavior:
      - Reads stored value for api_key (from file or default).
      - Resolves api_key by attempting os.environ.get(stored_value, stored_value).
        That means the stored value may be an environment variable name or the literal key.
      - Returns a dict with keys: base_url, model_name, api_key, include_schema_in_prompt.
    """
    # default values per product request
    defaults = {
        "base_url": "https://ark.cn-beijing.volces.com/api/v3",
        "model_name": "doubao-seed-1-6-250615",
        "api_key": "ARK_API_KEY",
        # new defaults controlling schema inclusion in AI prompts
        "include_schema_in_prompt": True,
    }

    if not AI_SETTINGS_PATH.exists():
        stored = defaults["api_key"]
        api_key = os.environ.get(stored, stored) if stored else ""
        return {
            "base_url": defaults["base_url"],
            "model_name": defaults["model_name"],
            "api_key": api_key,
            "include_schema_in_prompt": defaults["include_schema_in_prompt"],
        }

    try:
        with open(AI_SETTINGS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                data = {}
    except Exception:
        stored = defaults["api_key"]
        api_key = os.environ.get(stored, stored) if stored else ""
        return {
            "base_url": defaults["base_url"],
            "model_name": defaults["model_name"],
            "api_key": api_key,
            "include_schema_in_prompt": defaults["include_schema_in_prompt"],
        }

    # Accept either 'base_url' or legacy 'api_url' key
    base_url = str(data.get("base_url") or data.get("api_url") or defaults["base_url"]) or defaults["base_url"]
    model_name = str(data.get("model_name") or defaults["model_name"]) or defaults["model_name"]

    stored_api_key = data.get("api_key") if "api_key" in data else data.get("apiKey") if "apiKey" in data else defaults["api_key"]
    stored_api_key = str(stored_api_key) if stored_api_key is not None else ""

    api_key = os.environ.get(stored_api_key, stored_api_key) if stored_api_key else ""

    # New settings with safe casting and defaults
    include_schema = data.get("include_schema_in_prompt") if "include_schema_in_prompt" in data else defaults["include_schema_in_prompt"]
    try:
        include_schema = bool(include_schema)
    except Exception:
        include_schema = defaults["include_schema_in_prompt"]

    return {
        "base_url": base_url,
        "model_name": model_name,
        "api_key": api_key,
        "include_schema_in_prompt": include_schema,
    }


def save_ai_settings(settings: Dict[str, Any]) -> None:
    """Save AI settings to ai_settings.json. Expects a dict with keys base_url, model_name, api_key.

    This is a simple local storage; for production consider using OS keyring for secrets.
    """
    try:
        data = {
            "base_url": str(settings.get("base_url") or settings.get("api_url") or ""),
            "model_name": str(settings.get("model_name") or ""),
            "api_key": str(settings.get("api_key") or ""),
            "include_schema_in_prompt": bool(settings.get("include_schema_in_prompt", True)),
        }
        with open(AI_SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        raise


def load_app_state() -> dict:
    """Load simple application state from app_state.json.

    Returns a dict; on error or missing file returns empty dict.
    Currently used to persist last SQL editor content between sessions.
    """
    state_path = CONFIG_DIR / "app_state.json"
    if not state_path.exists():
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        # best-effort: ignore errors and return empty state
        pass
    return {}


def save_app_state(state: dict) -> None:
    """Save application state (dict) to app_state.json. Raises on write failures.

    Keep this simple; callers may catch exceptions if they want to ignore failures.
    """
    try:
        state_path = CONFIG_DIR / "app_state.json"
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state or {}, f, indent=2)
    except Exception:
        # bubble up so callers may decide to notify user; keep minimal here
        raise
