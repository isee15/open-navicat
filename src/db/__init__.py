"""Database package for CatAIDBViewer.

Creating this empty __init__ makes the `db` directory a package so
imports such as `from db.metadata import ...` resolve correctly both
in development and when bundled with PyInstaller.
"""

__all__ = [
    "connection",
    "executor",
    "metadata",
]
