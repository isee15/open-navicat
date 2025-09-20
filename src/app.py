from PyQt6.QtWidgets import QApplication
import sys

# If running from a PyInstaller onefile bundle, ensure the unpacked 'src' folder is on sys.path
# so imports like 'from db.metadata import ...' that rely on the project 'src' layout succeed.
try:
    if getattr(sys, 'frozen', False):
        meipass = getattr(sys, '_MEIPASS', None)
        if meipass:
            import os
            src_bundle_path = os.path.join(meipass, 'src')
            if os.path.isdir(src_bundle_path) and src_bundle_path not in sys.path:
                sys.path.insert(0, src_bundle_path)
except Exception:
    # best-effort: do not block startup if this fails
    pass

from main_window import MainWindow

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def _configure_logging():
    """Set up a simple logging configuration for development.

    - Logs DEBUG+ to console via basicConfig
    - Also writes to a rotating file under the user's .catdbviewer/logs folder
    """
    # Console + basic formatting with more detailed info
    logging.basicConfig(
        level=logging.DEBUG, 
        format='%(asctime)s %(levelname)-8s %(name)-20s: %(message)s',
        force=True  # Override any existing basicConfig
    )

    # Set specific loggers to DEBUG to ensure we see our messages
    logging.getLogger('db.connection').setLevel(logging.DEBUG)
    logging.getLogger('db.metadata').setLevel(logging.DEBUG)
    logging.getLogger('db.executor').setLevel(logging.DEBUG)
    logging.getLogger('main_window').setLevel(logging.DEBUG)
    logging.getLogger('__main__').setLevel(logging.DEBUG)

    # Ensure log directory exists
    log_dir = Path.home() / '.catdbviewer' / 'logs'
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / 'catdbviewer.log'
        handler = RotatingFileHandler(str(log_file), maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8')
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(name)-20s: %(message)s')
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
        print(f"File logging configured: {log_file}")
    except Exception as e:
        # best-effort: if file logging cannot be configured, continue with console logging only
        print(f"Failed to configure file logger: {e}")
        logging.getLogger(__name__).exception('Failed to configure file logger')


def _apply_qss(app):
    """Apply a QSS stylesheet if present.

    Resolution order (best-effort):
    1. If running from a PyInstaller onefile bundle, look in sys._MEIPASS/ui/ios_style.qss
    2. Look for ui/ios_style.qss next to the source file (development mode)
    3. If ui is available as a package, try importlib.resources to read the text
    Any failure is logged but does not prevent application startup.
    """
    try:
        import sys
        from pathlib import Path
        import logging
        # Try PyInstaller runtime folder first
        base = Path(getattr(sys, '_MEIPASS', Path(__file__).resolve().parent))
        qss_path = base / 'ui' / 'ios_style.qss'
        if qss_path.exists():
            with qss_path.open('r', encoding='utf-8') as f:
                app.setStyleSheet(f.read())
            return

        # Fallback: source tree next to this file (development)
        src_qss = Path(__file__).resolve().parent / 'ui' / 'ios_style.qss'
        if src_qss.exists():
            with src_qss.open('r', encoding='utf-8') as f:
                app.setStyleSheet(f.read())
            return

        # Last resort: try to read from package resources if ui is installed as a package
        try:
            import importlib.resources as pkg_resources
            try:
                # 'ui' package inside project (make ui a package by adding __init__.py)
                if pkg_resources.files('ui'):
                    q = pkg_resources.files('ui').joinpath('ios_style.qss')
                    if q.is_file():
                        txt = q.read_text(encoding='utf-8')
                        app.setStyleSheet(txt)
                        return
            except Exception:
                # ignore package resource failures
                pass
        except Exception:
            pass

        logging.getLogger(__name__).debug('QSS stylesheet not found in bundled or source locations')
    except Exception:
        logging.getLogger(__name__).exception('Failed to apply QSS stylesheet')


def main():
    _configure_logging()
    app = QApplication(sys.argv)
    # Apply optional iOS-like QSS theme if available
    _apply_qss(app)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()