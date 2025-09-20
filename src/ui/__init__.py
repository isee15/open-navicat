"""UI package for CatAIDBViewer.

This module ensures the `ui` directory is a proper Python package so that
imports and package resource lookups behave consistently in development
and when bundled with PyInstaller.
"""

__all__ = [
    "ai_settings_dialog",
    "connection_dialog",
    "schema_viewer",
]
