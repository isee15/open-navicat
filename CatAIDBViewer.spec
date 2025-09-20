# -*- mode: python ; coding: utf-8 -*-
import os
from PyInstaller.utils.hooks import collect_all, collect_submodules

# Collect PyQt6 resources (data files, binaries, and hidden imports/plugins)
pyqt_datas, pyqt_binaries, pyqt_hiddenimports = collect_all('PyQt6')

# Collect SQLAlchemy dynamic submodules to help PyInstaller find dialects
sqlalchemy_subs = collect_submodules('sqlalchemy')

# Common hidden imports for DB drivers and runtime helpers
hiddenimports = list(set(
    pyqt_hiddenimports + sqlalchemy_subs + [
        'psycopg2',
        'psycopg2._psycopg',
        'pymysql',
        'mysql.connector',
        'importlib.resources',
        'pkg_resources',
        'sqlalchemy.dialects.postgresql',
        'sqlalchemy.dialects.mysql',
        'sqlalchemy.dialects.sqlite',
    ]
))

# Include the project's ui package directory as data so QSS and other resources are available
datas = pyqt_datas + [(os.path.join('src', 'ui'), 'ui')]

# Include PyQt6 binaries (Qt libraries / plugins)
binaries = pyqt_binaries

# Analysis - root entry point is src/app.py
a = Analysis(
    ['src/app.py'],
    pathex=[os.path.abspath('.')],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

# Create an EXE then COLLECT into an onedir bundle for easier debugging of runtime issues
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    exclude_binaries=True,
    name='CatAIDBViewer',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    name='CatAIDBViewer',
)
