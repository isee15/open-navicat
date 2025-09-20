from PyQt6.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QFileDialog,
    QMessageBox,
    QTreeWidget,
    QTreeWidgetItem,
    QSplitter,
    QTabWidget,
    QDialog,
    QStyle,
)
from PyQt6.QtGui import QAction, QIcon, QDesktopServices
from PyQt6.QtCore import Qt, QUrl
import traceback
import re
from sqlalchemy import inspect, text
import logging
import os

logger = logging.getLogger(__name__)

from editor.sql_editor import SqlEditor
from db.connection import ConnectionManager
from db.executor import execute_sql
from models.table_model import TableModel
from PyQt6.QtWidgets import QTableView, QPushButton, QComboBox, QMenu, QApplication
from utils.csv_export import export_to_csv
from ui.connection_dialog import ConnectionDialog
from ui.schema_viewer import SchemaViewerDialog
from utils.worker import ExecutionWorker
from ui.ai_settings_dialog import AISettingsDialog
from utils.settings import load_ai_settings, save_ai_settings, CONFIG_DIR, load_app_state, save_app_state
from db.metadata import clear_schema_cache, extract_first_table_from_select, get_pk_columns_for_table
from db.executor import apply_updates, delete_row


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CatAIDBViewer(by 乖猫记账)")
        self.resize(1000, 700)

        # Do not apply hardcoded inline stylesheet here; the app-wide QSS in ui/ios_style.qss
        # controls the theme (light/dark). This keeps colors consistent and responsive
        # to the dark-mode toggle implemented via dynamic property 'dark' on widgets.

        self.conn_mgr = ConnectionManager()

        # Expose the active ConnectionManager to other modules (e.g., db.metadata._find_engine)
        # so that metadata introspection reuses the application's engines instead of constructing
        # a new manager from saved configs. This ensures the AI prompt reflects the currently
        # selected datasource (e.g., MySQL vs PostgreSQL).
        try:
            import db.connection as _db_conn_mod
            _db_conn_mod.conn_mgr = self.conn_mgr
        except Exception:
            # best-effort: do not fail UI init if assignment fails
            pass

        self._init_actions()
        self._init_ui()
        # Restore simple app state (load last SQL from previous session into first tab)
        try:
            try:
                self._restore_app_state()
            except Exception:
                pass
        except Exception:
            pass

    def _init_actions(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("File")

        open_sqlite_action = QAction("Open SQLite Database", self)
        open_sqlite_action.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogOpenButton))
        open_sqlite_action.triggered.connect(self.open_sqlite_db)
        file_menu.addAction(open_sqlite_action)

        new_tab_action = QAction("New SQL Tab", self)
        new_tab_action.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon))
        new_tab_action.triggered.connect(self.new_sql_tab)
        file_menu.addAction(new_tab_action)

        exit_action = QAction("Exit", self)
        exit_action.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogCloseButton))
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Connection menu
        conn_menu = menubar.addMenu("Connection")
        new_conn_action = QAction("New Connection...", self)
        new_conn_action.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DriveNetIcon))
        new_conn_action.triggered.connect(self.open_new_connection_dialog)
        conn_menu.addAction(new_conn_action)

        schema_action = QAction("Schema Viewer", self)
        schema_action.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogContentsView))
        schema_action.triggered.connect(self.open_schema_viewer_for_selected)
        conn_menu.addAction(schema_action)

        # Settings menu for application-wide preferences (AI config etc.)
        settings_menu = menubar.addMenu("Settings")
        ai_settings_action = QAction("AI Settings...", self)
        ai_settings_action.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView))
        ai_settings_action.triggered.connect(self.open_ai_settings_dialog)
        settings_menu.addAction(ai_settings_action)

        # Dark mode toggle (checkable)
        self._dark_mode_action = QAction("Dark Mode", self)
        try:
            self._dark_mode_action.setCheckable(True)
            # shortcut for quick toggling
            self._dark_mode_action.setShortcut('Ctrl+D')
            self._dark_mode_action.toggled.connect(lambda checked: self.apply_dark_mode(checked))
            settings_menu.addAction(self._dark_mode_action)
        except Exception:
            # best-effort: ignore environments that don't support shortcuts
            try:
                settings_menu.addAction(self._dark_mode_action)
            except Exception:
                pass

        # Open config directory quickly from menu
        open_config_action = QAction("Open Config Folder", self)
        try:
            open_config_action.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DirOpenIcon))
        except Exception:
            pass
        open_config_action.setToolTip("Open the application's configuration directory")
        open_config_action.triggered.connect(self.open_config_folder)
        settings_menu.addAction(open_config_action)

    def _init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)

        # Left: tree showing connections and their tables
        self.conn_tree = QTreeWidget()
        self.conn_tree.setMaximumWidth(320)
        self.conn_tree.setColumnCount(1)
        self.conn_tree.setHeaderHidden(True)
        self.conn_tree.itemDoubleClicked.connect(self.on_left_item_double_clicked)
        # Slightly larger font for the tree for readability
        self.conn_tree.setStyleSheet("QTreeWidget { font-size: 12px; }")

        # Enable custom context menu for editing/removing top-level connection nodes
        self.conn_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.conn_tree.customContextMenuRequested.connect(self._show_connection_context_menu)

        # Populate existing connections from config (as top-level nodes)
        for name in self.conn_mgr.list_connections():
            self._add_connection_item(name)

        # Right: splitter with SQL tabs
        self.editor_tabs = QTabWidget()
        # Allow closing SQL editor tabs and handle cleanup (e.g., cancel running queries)
        self.editor_tabs.setTabsClosable(True)
        self.editor_tabs.tabCloseRequested.connect(self._on_editor_tab_close_requested)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(self.conn_tree)
        splitter.addWidget(self.editor_tabs)
        splitter.setStretchFactor(1, 1)

        layout.addWidget(splitter)

        # Start with one tab
        self.new_sql_tab()

        # Restore dark mode preference if present in saved state
        try:
            state = load_app_state() or {}
            if isinstance(state, dict) and state.get('dark_mode'):
                try:
                    # ensure action exists and reflects state
                    if getattr(self, '_dark_mode_action', None):
                        self._dark_mode_action.setChecked(True)
                    self.apply_dark_mode(True)
                except Exception:
                    pass
        except Exception:
            pass

    def apply_dark_mode(self, enabled: bool) -> None:
        """Enable or disable dark mode by setting a dynamic 'dark' property on
        the main window and its descendants and forcing a style refresh. This
        leverages the existing QSS selectors that target QWidget[dark="true"].
        """
        try:
            # store for persistence
            self._dark_mode_enabled = bool(enabled)
            # recursively set property on main window and all child widgets
            def _set_prop(w):
                try:
                    w.setProperty('dark', enabled)
                except Exception:
                    pass
                for ch in w.findChildren(QWidget):
                    try:
                        ch.setProperty('dark', enabled)
                    except Exception:
                        pass

            _set_prop(self)
            # Force the style engine to re-evaluate the stylesheet
            try:
                app = QApplication.instance()
                if app is not None:
                    app.setStyleSheet(app.styleSheet())
            except Exception:
                pass
        except Exception:
            pass

    def _add_connection_item(self, name: str):
        """Add a top-level connection node and attempt to load its tables as children."""
        root = QTreeWidgetItem(self.conn_tree, [name])
        root.setData(0, Qt.ItemDataRole.UserRole, ("connection", name))
        root.setExpanded(False)
        try:
            # use platform style icon for connection
            root.setIcon(0, self.style().standardIcon(QStyle.StandardPixmap.SP_DirIcon))
        except Exception:
            pass
        # try to load tables immediately; failure is handled per-connection
        try:
            engine = self.conn_mgr.get_connection(name)
            inspector = inspect(engine)
            # Detailed debug logging to diagnose missing tables
            try:
                cfg = getattr(self.conn_mgr, '_configs', {}).get(name, {})
            except Exception:
                logger.debug("Loading tables for connection %s; no config available", name)
            try:
                eng_url = getattr(engine, 'url', None)
                logger.debug("Engine for %s created: %r", name, eng_url)
            except Exception:
                logger.debug("Engine for %s created (no URL available)", name)
            # log available schemas to help diagnose search_path issues
            try:
                schemas = inspector.get_schema_names()
                logger.debug("Inspector schemas for %s: %r", name, schemas)
            except Exception:
                logger.debug("Could not fetch schema names for %s", name)
            # If a schema/search_path was stored with the connection config, prefer listing that schema
            schema = None
            try:
                cfg = getattr(self.conn_mgr, '_configs', {}).get(name, {})
                if isinstance(cfg, dict):
                    schema = cfg.get('schema') or (cfg.get('params') or {}).get('schema')
            except Exception:
                schema = None
            # Ask the inspector for tables using explicit schema when available.
            if schema:
                try:
                    tables = inspector.get_table_names(schema=schema)
                except Exception:
                    logger.exception("Failed to get table names for %s with schema=%r; falling back to default schema listing", name, schema)
                    # fallback to default listing and log result
                    try:
                        tables = inspector.get_table_names()
                    except Exception:
                        logger.exception("Fallback get_table_names() also failed for %s", name)
                        raise
            else:
                tables = inspector.get_table_names()
            # If no tables found, log schemas + params for debugging
            try:
                if not tables:
                    logger.debug("No tables returned for %s (schema=%r); inspector schemas=%r; config=%r", name, schema, getattr(inspector, 'get_schema_names', lambda: None)(), getattr(self.conn_mgr, '_configs', {}).get(name))
            except Exception:
                pass
            tables.sort()
            for t in tables:
                child = QTreeWidgetItem(root, [t])
                child.setData(0, Qt.ItemDataRole.UserRole, ("table", t, name))
                try:
                    child.setIcon(0, self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon))
                except Exception:
                    pass
        except Exception as e:
            # indicate failure to load tables (user can still double-click to open schema viewer)
            logger.exception(f"Failed to load tables for connection: {name} error: {e}")  
            err = QTreeWidgetItem(root, ["<failed to load tables>"])
            err.setData(0, Qt.ItemDataRole.UserRole, ("error", None))
            try:
                err.setIcon(0, self.style().standardIcon(QStyle.StandardPixmap.SP_MessageBoxCritical))
            except Exception:
                pass

    def _load_tables_for_connection(self, root_item: QTreeWidgetItem, connection_name: str):
        """(Re)load table children for a given connection node."""
        # clear existing children
        root_item.takeChildren()
        try:
            engine = self.conn_mgr.get_connection(connection_name)
            inspector = inspect(engine)
            # Detailed debug logging for interactive reloads
            try:
                cfg = getattr(self.conn_mgr, '_configs', {}).get(connection_name, {})
            except Exception:
                logger.debug("Reloading tables for connection %s; no config available", connection_name)
            try:
                eng_url = getattr(engine, 'url', None)
                logger.debug("Engine for %s: %r", connection_name, eng_url)
            except Exception:
                logger.debug("Engine for %s available (no URL)", connection_name)
            try:
                schemas = inspector.get_schema_names()
                logger.debug("Inspector schemas for %s: %r", connection_name, schemas)
            except Exception:
                logger.debug("Could not fetch schema names for %s", connection_name)
            schema = None
            try:
                cfg = getattr(self.conn_mgr, '_configs', {}).get(connection_name, {})
                if isinstance(cfg, dict):
                    schema = cfg.get('schema') or (cfg.get('params') or {}).get('schema')
            except Exception:
                schema = None
            if schema:
                try:
                    tables = inspector.get_table_names(schema=schema)
                except Exception:
                    logger.exception("Failed to get table names for %s with schema=%r; trying default", connection_name, schema)
                    try:
                        tables = inspector.get_table_names()
                    except Exception:
                        logger.exception("Fallback get_table_names() failed for %s", connection_name)
                        raise
            else:
                tables = inspector.get_table_names()
            try:
                if not tables:
                    logger.debug("No tables returned for %s (schema=%r); inspector schemas=%r; config=%r", connection_name, schema, getattr(inspector, 'get_schema_names', lambda: None)(), getattr(self.conn_mgr, '_configs', {}).get(connection_name))
            except Exception:
                pass
            tables.sort()
            for t in tables:
                child = QTreeWidgetItem(root_item, [t])
                child.setData(0, Qt.ItemDataRole.UserRole, ("table", t, connection_name))
                try:
                    child.setIcon(0, self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon))
                except Exception:
                    pass
        except Exception as e:
            err = QTreeWidgetItem(root_item, ["<failed to load tables"])
            err.setData(0, Qt.ItemDataRole.UserRole, ("error", None))
            logger.exception(f"Failed to load tables for connection: {connection_name} error: {e}")
            try:
                err.setIcon(0, self.style().standardIcon(QStyle.StandardPixmap.SP_MessageBoxCritical))
            except Exception:
                pass

    def on_left_item_double_clicked(self, item: QTreeWidgetItem, column: int):
        """Handle double-clicks on the left tree: table -> open 100 rows; connection -> expand/collapse or open schema viewer."""
        data = item.data(0, Qt.ItemDataRole.UserRole)
        # table node
        if data and data[0] == 'table':
            table_name = data[1]
            conn_name = data[2] if len(data) > 2 else (item.parent().text(0) if item.parent() else None)
            if conn_name:
                self._on_table_activated(conn_name, table_name)
            return

        # connection node: toggle expand and try to (re)load tables
        if data and data[0] == 'connection':
            conn_name = data[1]
            # reload to ensure fresh metadata
            try:
                self._load_tables_for_connection(item, conn_name)
            except Exception:
                pass
            item.setExpanded(not item.isExpanded())
            return

        # other nodes: ignore
        return

    def open_sqlite_db(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select SQLite database file", "", "SQLite Files (*.db *.sqlite);;All Files (*)")
        if not path:
            return
        try:
            name = self.conn_mgr.add_sqlite_connection(path)
            self._add_connection_item(name)
            QMessageBox.information(self, "Connected", f"Added connection: {name}")
        except Exception as e:
            tb = traceback.format_exc()
            msg = str(e)
            dlg = QMessageBox(self)
            dlg.setIcon(QMessageBox.Icon.Critical)
            dlg.setWindowTitle("Error")
            dlg.setText(f"Failed to add connection: {msg}")
            dlg.setDetailedText(tb)
            dlg.exec()

    def new_sql_tab(self, name: str | None = None):
        # Each tab contains vertical splitter: editor on top, results tabs below
        container = QWidget()
        vlayout = QVBoxLayout(container)
        # toolbar
        toolbar_layout = QHBoxLayout()
        run_btn = QPushButton("Run")
        try:
            run_btn.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPlay))
        except Exception:
            pass
        run_btn.setToolTip("Execute SQL in editor and append results as a new result tab")
        toolbar_layout.addWidget(run_btn)
        # Connection selector: allows user to pick which connection to run the SQL against
        conn_combo = QComboBox()
        conn_combo.setToolTip("Select connection datasource for this SQL tab")
        conn_combo.setMinimumWidth(180)
        # default empty choice
        conn_combo.addItem("")
        try:
            for cname in self.conn_mgr.list_connections():
                conn_combo.addItem(cname)
        except Exception:
            pass
        # preselect current connection if available
        try:
            cur = self._selected_connection_name()
            if cur:
                idx = conn_combo.findText(cur)
                if idx >= 0:
                    conn_combo.setCurrentIndex(idx)
        except Exception:
            pass
        # When the user switches the selected connection for this tab, attempt to apply
        # any saved schema/search_path for that connection (e.g., PostgreSQL search_path)
        # and refresh the connection's table list in the left tree so the UI reflects
        # the chosen schema.
        try:
            conn_combo.currentIndexChanged.connect(lambda _idx, combo=conn_combo, cont=container if 'container' in locals() else None: self._on_tab_connection_changed(cont, combo))
        except Exception:
            pass
        toolbar_layout.addWidget(conn_combo)
        export_btn = QPushButton("Export CSV")
        try:
            export_btn.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogSaveButton))
        except Exception:
            pass
        export_btn.setToolTip("Export current result tab to CSV")
        toolbar_layout.addWidget(export_btn)
        cancel_btn = QPushButton("Cancel")
        try:
            cancel_btn.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogCancelButton))
        except Exception:
            pass
        cancel_btn.setToolTip("Cancel running query")
        cancel_btn.setEnabled(False)
        toolbar_layout.addWidget(cancel_btn)
        toolbar_layout.addStretch()
        vlayout.addLayout(toolbar_layout)

        editor = SqlEditor()
        vlayout.addWidget(editor)

        results_tabs = QTabWidget()
        results_tabs.setTabsClosable(True)
        results_tabs.tabCloseRequested.connect(lambda idx, rt=results_tabs: rt.removeTab(idx))
        vlayout.addWidget(results_tabs)

        # create tab
        title = name or f"SQL-{self.editor_tabs.count() + 1}"
        self.editor_tabs.addTab(container, title)
        # make the new editor tab active so it receives focus immediately
        self.editor_tabs.setCurrentIndex(self.editor_tabs.count() - 1)

        # attach worker placeholder
        container._worker = None
        # expose editor/results for external control
        container._editor = editor
        container._results_tabs = results_tabs
        # remember original tab title for running indicator
        container._tab_title = title
        # expose connection combo for run logic
        container._conn_combo = conn_combo

        # wire up run
        def on_run():
            # prevent double-run
            if getattr(container, '_worker', None) is not None:
                QMessageBox.information(self, "Running", "A query is already running in this tab.")
                return
            sql = editor.get_sql()

            # allow switching datasource via a leading comment or a USE CONNECTION statement:
            # e.g. -- connection: myconn  OR  USE CONNECTION myconn;
            conn_name = None
            try:
                m = re.search(r"^\s*--\s*connection\s*:\s*([^\s]+)", sql, re.IGNORECASE | re.MULTILINE)
                if m:
                    conn_name = m.group(1)
                    # remove the directive line so it isn't executed
                    sql = re.sub(r"^\s*--\s*connection\s*:\s*[^\n]+\n?", "", sql, count=1, flags=re.IGNORECASE | re.MULTILINE)
                else:
                    m2 = re.search(r"^\s*USE\s+CONNECTION\s+([^\s;]+)\s*;?", sql, re.IGNORECASE | re.MULTILINE)
                    if m2:
                        conn_name = m2.group(1)
                        sql = re.sub(r"^\s*USE\s+CONNECTION\s+[^\n;]+\s*;?\n?", "", sql, count=1, flags=re.IGNORECASE | re.MULTILINE)
            except Exception:
                conn_name = None

            # fallback to currently selected connection if none specified in SQL
            if conn_name is None:
                # prefer connection selected in the tab's combobox if present
                try:
                    combo = getattr(container, '_conn_combo', None)
                    if combo is not None:
                        txt = combo.currentText().strip()
                        if txt:
                            conn_name = txt
                except Exception:
                    pass
                # fallback to currently selected connection in the tree
                if conn_name is None:
                    conn_name = self._selected_connection_name()

            if connection_name := conn_name:
                try:
                    conn = self.conn_mgr.get_connection(connection_name)
                except Exception as e:
                    QMessageBox.warning(self, "No connection", f"Connection '{connection_name}' is not available: {e}")
                    return
            else:
                QMessageBox.warning(self, "No connection", "Please open or select a connection from the left and try again.")
                return

            # set running indicator in tab and status bar
            try:
                idx = self.editor_tabs.indexOf(container)
                self.editor_tabs.setTabText(idx, f"⏳ {container._tab_title}")
            except Exception:
                pass
            try:
                self.statusBar().showMessage(f"Running query on '{connection_name}'...", 0)
            except Exception:
                pass

            # Run in background worker
            worker = ExecutionWorker(conn, sql)
            container._worker = worker

            def on_results(results):
                # results is a list of tuples returned by execute_sql. Each tuple may be
                # (columns, rows, elapsed_seconds, truncated) or older variants.
                for i, res in enumerate(results, start=1):
                    # support multiple tuple shapes for backward compatibility
                    columns = rows = None
                    elapsed = None
                    truncated = False
                    try:
                        if isinstance(res, (list, tuple)) and len(res) == 4:
                            columns, rows, elapsed, truncated = res
                        elif isinstance(res, (list, tuple)) and len(res) == 3:
                            columns, rows, elapsed = res
                        elif isinstance(res, (list, tuple)) and len(res) == 2:
                            columns, rows = res
                        else:
                            # unexpected shape: treat whole result as a single message column
                            columns = ["Message"]
                            rows = [ (str(res),) ]
                    except Exception:
                        # fallback to safe defaults
                        columns = ["Message"]
                        rows = [ (str(res),) ]

                    # If this is a simple message row (non-SELECT), show it as a small result tab
                    # but also display truncation as a status bar hint when flagged.
                    if columns == ["Message"] and rows:
                        try:
                            first_cell = rows[0][0] if isinstance(rows[0], (list, tuple)) else rows[0]
                        except Exception:
                            first_cell = None
                        # show truncation notice in status bar if flagged
                        if truncated:
                            try:
                                self.statusBar().showMessage("Query result truncated (showing partial rows)", 10000)
                            except Exception:
                                pass
                        # continue and add the message as a normal small result tab

                    # Use sqlparse-based extractor to detect primary target table from the SQL and
                    # ask the metadata layer for PK columns. This is best-effort and avoids ad-hoc regex.
                    table_name = None
                    pk_cols = []
                    try:
                        table_name = extract_first_table_from_select(sql)
                        if table_name:
                            try:
                                pk_cols = get_pk_columns_for_table(conn, table_name) or []
                            except Exception:
                                pk_cols = []
                    except Exception:
                        table_name = None
                        pk_cols = []

                    model = TableModel(columns, rows, pk_columns=pk_cols)
                    view = QTableView()
                    try:
                        view.setEditTriggers(QTableView.EditTrigger.DoubleClicked | QTableView.EditTrigger.SelectedClicked | QTableView.EditTrigger.EditKeyPressed)
                    except Exception:
                        # best-effort: ignore if flags differ across PyQt versions
                        pass

                    view.setModel(model)
                    view.resizeColumnsToContents()

                    # Wrap view in a container that also provides Save / Delete buttons
                    container_widget = QWidget()
                    container_layout = QVBoxLayout(container_widget)
                    container_layout.setContentsMargins(0, 0, 0, 0)
                    container_layout.addWidget(view)
                    btn_bar = QHBoxLayout()
                    save_btn = QPushButton("Save")
                    save_btn.setToolTip("Save pending edits to database")
                    delete_btn = QPushButton("Delete")
                    delete_btn.setToolTip("Delete selected row from database")
                    btn_bar.addWidget(save_btn)
                    btn_bar.addWidget(delete_btn)
                    btn_bar.addStretch()
                    container_layout.addLayout(btn_bar)

                    def on_save():
                        if not model.has_pending_changes():
                            QMessageBox.information(self, "No changes", "There are no pending changes to save.")
                            return
                        if not table_name:
                            QMessageBox.warning(self, "Cannot save", "Target table could not be determined for this result; cannot perform UPDATE.")
                            return
                        pending = model.get_pending_changes()
                        if not pending:
                            QMessageBox.information(self, "No changes", "There are no pending cell edits to save.")
                            return
                        try:
                            # Delegate applying updates to db.executor.apply_updates which handles transactions and param binding
                            rows_affected = apply_updates(conn, table_name, pending)
                            model.clear_pending_changes()
                            QMessageBox.information(self, "Saved", f"Saved changes. Rows affected: {rows_affected}")
                        except Exception as e:
                            tb = traceback.format_exc()
                            QMessageBox.critical(self, "Save error", f"Failed to save changes: {e}\n\nDetails:\n{tb}")

                    def on_delete():
                        sel = view.selectionModel().selectedRows()
                        if not sel:
                            QMessageBox.information(self, "No selection", "Select a row to delete.")
                            return
                        row = sel[0].row()
                        if not table_name:
                            QMessageBox.warning(self, "Cannot delete", "Target table could not be determined for this result; cannot perform DELETE.")
                            return
                        pk_vals = model.get_pk_values_for_row(row)
                        if not pk_vals:
                            QMessageBox.warning(self, "Cannot delete", "Could not determine primary key values for the selected row.")
                            return
                        resp = QMessageBox.question(self, "Delete row", "Delete the selected row from the database?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                        if resp != QMessageBox.StandardButton.Yes:
                            return
                        try:
                            deleted = delete_row(conn, table_name, pk_vals)
                            if deleted:
                                model.mark_row_deleted(row)
                                QMessageBox.information(self, "Deleted", "Row deleted.")
                            else:
                                QMessageBox.warning(self, "Delete", "No rows deleted (row may no longer exist).")
                        except Exception as e:
                            tb = traceback.format_exc()
                            QMessageBox.critical(self, "Delete error", f"Failed to delete row: {e}\n\nDetails:\n{tb}")

                    save_btn.clicked.connect(on_save)
                    delete_btn.clicked.connect(on_delete)

                    # compute result summary (title and tooltip)
                    try:
                        row_count = len(rows) if rows is not None else 0
                    except Exception:
                        row_count = 0

                    title_parts = [f"Result {results_tabs.count() + 1}", f"{row_count} rows"]
                    if elapsed is not None:
                        try:
                            title_parts.append(f"{elapsed:.3f}s")
                        except Exception:
                            pass
                    if truncated:
                        title_parts.append("(truncated)")
                    tab_title = " — ".join(title_parts)

                    tooltip = f"Rows: {row_count}"
                    if elapsed is not None:
                        try:
                            tooltip += f"\nElapsed: {elapsed:.6f} seconds"
                        except Exception:
                            pass
                    if truncated:
                        tooltip += "\nResult truncated: only a subset of rows is shown"

                    results_tabs.addTab(container_widget, tab_title)
                    container_widget.setToolTip(tooltip)
                    results_tabs.setCurrentIndex(results_tabs.count() - 1)

                # show a brief status message after processing results
                try:
                    # summarize last result if available
                    last = results[-1] if results else None
                    if last and isinstance(last, (list, tuple)) and len(last) >= 2:
                        rows_last = last[1] or []
                        elapsed_last = last[2] if len(last) > 2 else None
                        rc = len(rows_last)
                        if elapsed_last is not None:
                            self.statusBar().showMessage(f"Query finished: {rc} rows in {elapsed_last:.3f}s", 5000)
                        else:
                            self.statusBar().showMessage(f"Query finished: {rc} rows", 5000)
                except Exception:
                    pass

            def on_error(msg: str):
                # provide cancellation feedback if applicable
                try:
                    if 'cancel' in msg.lower():
                        self.statusBar().showMessage("Query canceled", 5000)
                    else:
                        self.statusBar().showMessage("Query error: see dialog", 5000)
                except Exception:
                    pass
                QMessageBox.critical(self, "Execution error", msg)

            def on_finished():
                container._worker = None
                run_btn.setEnabled(True)
                cancel_btn.setEnabled(False)
                # restore tab title and clear running indicator
                try:
                    idx2 = self.editor_tabs.indexOf(container)
                    self.editor_tabs.setTabText(idx2, container._tab_title)
                except Exception:
                    pass
                try:
                    # if the status bar still shows a running message leave a brief ready message
                    self.statusBar().showMessage("Ready", 2000)
                except Exception:
                    pass

            worker.results_ready.connect(on_results)
            worker.error.connect(on_error)
            worker.finished_signal.connect(on_finished)

            run_btn.setEnabled(False)
            cancel_btn.setEnabled(True)
            worker.start()

        run_btn.clicked.connect(on_run)

        def export_current_results():
            idx = results_tabs.currentIndex()
            if idx < 0:
                QMessageBox.information(self, "No results", "No result tab to export.")
                return
            widget = results_tabs.widget(idx)
            model = None
            try:
                model = widget.model()
            except Exception:
                model = None
            if model is None:
                QMessageBox.warning(self, "Export failed", "Selected result does not contain a table model.")
                return
            # prefer model.get_all_data if available
            if hasattr(model, 'get_all_data'):
                columns, rows = model.get_all_data()
            else:
                # fallback: extract from QAbstractItemModel
                cols = model.columnCount()
                rows_count = model.rowCount()
                columns = [model.headerData(c, Qt.Orientation.Horizontal) for c in range(cols)]
                rows = []
                for r in range(rows_count):
                    row = []
                    for c in range(cols):
                        idx2 = model.index(r, c)
                        val = model.data(idx2, Qt.ItemDataRole.DisplayRole)
                        row.append(val)
                    rows.append(tuple(row))

            path, _ = QFileDialog.getSaveFileName(self, "Save CSV", "results.csv", "CSV Files (*.csv);;All Files (*)")
            if not path:
                return
            try:
                export_to_csv(columns, rows, path)
                QMessageBox.information(self, "Exported", f"Exported to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Export error", str(e))

        export_btn.clicked.connect(export_current_results)

        # cancel running query
        def on_cancel():
            worker = getattr(container, '_worker', None)
            if worker is not None:
                worker.stop()
                try:
                    self.statusBar().showMessage("Cancel requested...", 5000)
                except Exception:
                    pass

        cancel_btn.clicked.connect(on_cancel)
        # Allow Ctrl+Enter from the editor to execute as well
        try:
            editor.execute_requested.connect(on_run)
        except Exception:
            # if editor doesn't have the signal (very old version), ignore
            pass

        # return the container so callers may programmatically interact (e.g., trigger run)
        return container

    def _selected_connection_name(self) -> str | None:
        item = self.conn_tree.currentItem()
        if item:
            data = item.data(0, Qt.ItemDataRole.UserRole)
            if data and data[0] == 'connection':
                return data[1]
            if data and data[0] == 'table':
                # return the connection part
                return data[2]
            # fallback to textual label
            return item.text(0)
        # fallback to first connection if exists
        if self.conn_tree.topLevelItemCount() > 0:
            return self.conn_tree.topLevelItem(0).text(0)
        return None

    def _on_table_activated(self, connection_name: str, table: str):
        # create or focus an existing SQL tab for the table and populate the editor with SELECT ... LIMIT 100, then trigger execution
        try:
            # ensure connection is selected in UI so running code picks correct connection by default
            conn_item = self.find_connection_item(connection_name)
            if conn_item:
                self.conn_tree.setCurrentItem(conn_item)

            desired_title = f"{connection_name}/{table}"
            # If a tab with the same title exists, switch to it and return
            for i in range(self.editor_tabs.count()):
                if self.editor_tabs.tabText(i) == desired_title:
                    self.editor_tabs.setCurrentIndex(i)
                    return

            # create a new tab
            container = self.new_sql_tab(name=desired_title)
            if container is None:
                return
            editor = getattr(container, '_editor', None)
            if editor is None:
                # try to find child
                editor = container.findChild(SqlEditor)
            if editor is None:
                return
            # basic quoting for identifiers with spaces
            if ' ' in table or '-' in table:
                table_expr = f'"{table}"'
            else:
                table_expr = table
            sql = f"SELECT * FROM {table_expr} LIMIT 100"
            editor.set_sql(sql)
            # set the tab's connection combobox to the activated connection
            try:
                combo = getattr(container, '_conn_combo', None)
                if combo is not None:
                    idx = combo.findText(connection_name)
                    if idx >= 0:
                        combo.setCurrentIndex(idx)
            except Exception:
                pass
            # trigger execution via the editor signal
            try:
                editor.execute_requested.emit()
            except Exception:
                # fallback: do nothing
                pass
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to open table {table}: {e}")

    def open_new_connection_dialog(self):
        dlg = ConnectionDialog(self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        data = dlg.get_data()
        name = data.get('name') or f"{data.get('type')}@{data.get('host')}"
        # if user provided a JDBC URL and didn't enter a name, use a short form of the JDBC as name
        jdbc_val = data.get('jdbc')
        if not data.get('name') and jdbc_val:
            # take scheme://host:port/db as a fallback display name
            try:
                from urllib.parse import urlparse
                raw = jdbc_val
                if raw.startswith('jdbc:'):
                    raw = raw[len('jdbc:'):]
                p = urlparse(raw)
                short = f"{p.scheme}://{p.hostname or ''}:{p.port or ''}/{(p.path[1:] if p.path else '')}"
                name = short
            except Exception:
                pass
        try:
            logger.debug(f"Adding connection with parameters: {data}")
            # Support dialogs that return 'username' key; prefer it over 'user'
            user_val = data.get('username') or data.get('user')
            added = self.conn_mgr.add_connection(
                name,
                data.get('type'),
                host=data.get('host'),
                port=data.get('port'),
                user=user_val,
                password=data.get('password'),
                database=data.get('database'),
                jdbc=jdbc_val,
                schema=data.get('schema'),
            )
            self._add_connection_item(added)
            # Ensure existing editor tabs reflect the newly added connection
            try:
                self._refresh_connection_combos()
            except Exception:
                pass
            QMessageBox.information(self, "Connected", f"Added connection: {added}")
        except Exception as e:
            tb = traceback.format_exc()
            dlg = QMessageBox(self)
            dlg.setIcon(QMessageBox.Icon.Critical)
            dlg.setWindowTitle("Connection error")
            dlg.setText(str(e))
            dlg.setDetailedText(tb)
            dlg.exec()

    def open_schema_viewer_for_selected(self):
        name = self._selected_connection_name()
        if name is None:
            QMessageBox.information(self, "No connection", "Select a connection to view schema.")
            return
        try:
            engine = self.conn_mgr.get_connection(name)
            dlg = SchemaViewerDialog(engine, self)
            dlg.exec()
        except Exception as e:
            tb = traceback.format_exc()
            dlg = QMessageBox(self)
            dlg.setIcon(QMessageBox.Icon.Critical)
            dlg.setWindowTitle("Schema error")
            dlg.setText(str(e))
            dlg.setDetailedText(tb)
            dlg.exec()

    def open_ai_settings_dialog(self):
        """Open a dialog to configure AI settings (base URL, model name, API key) and save them locally."""
        try:
            current = load_ai_settings()
        except Exception:
            current = {"base_url": "", "model_name": "", "api_key": ""}
        dlg = AISettingsDialog(self, settings=current)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        data = dlg.get_data()
        try:
            save_ai_settings(data)
            QMessageBox.information(self, "Saved", "AI settings saved successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Save error", f"Failed to save AI settings: {e}")

    def open_config_folder(self):
        """Open the user configuration directory in the OS file manager."""
        try:
            path = CONFIG_DIR
            # ensure the directory exists
            try:
                path.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            # On Windows use os.startfile for best experience, otherwise use QDesktopServices
            if os.name == 'nt':
                try:
                    os.startfile(str(path))
                    return
                except Exception:
                    # fallback to QDesktopServices
                    pass
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))
        except Exception as e:
            try:
                QMessageBox.critical(self, "Open folder error", f"Failed to open config folder: {e}")
            except Exception:
                pass

    def find_connection_item(self, connection_name: str) -> QTreeWidgetItem | None:
        """Find a top-level connection node by its display name."""
        for i in range(self.conn_tree.topLevelItemCount()):
            it = self.conn_tree.topLevelItem(i)
            if it.text(0) == connection_name:
                return it
        return None

    def _show_connection_context_menu(self, pos):
        """Show a small context menu when right-clicking the connection tree. Only provides actions for top-level connection nodes."""
        try:
            item = self.conn_tree.itemAt(pos)
            if not item:
                return
            data = item.data(0, Qt.ItemDataRole.UserRole)
            # Only allow edit/delete on top-level connection nodes
            if not data or data[0] != 'connection':
                return
            conn_name = data[1]
            menu = QMenu(self)
            # Add a refresh action so users can explicitly reload the table list for a connection
            refresh_act = menu.addAction("Refresh Tables")
            try:
                refresh_act.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_BrowserReload))
            except Exception:
                pass
            edit_act = menu.addAction("Edit Connection")
            delete_act = menu.addAction("Delete Connection")

            action = menu.exec(self.conn_tree.viewport().mapToGlobal(pos))
            if action == refresh_act:
                try:
                    # prefer the known top-level item for this connection
                    node = self.find_connection_item(conn_name) or item
                    # clear any cached schema/metadata for the engine so we truly reload
                    try:
                        eng = None
                        try:
                            eng = self.conn_mgr.get_connection(conn_name)
                        except Exception:
                            eng = None
                        if eng is not None:
                            try:
                                clear_schema_cache(eng)
                            except Exception:
                                # best-effort: ignore cache-clear failures
                                pass
                    except Exception:
                        pass
                    # briefly collapse to make reload visible; ignore failures
                    try:
                        node.setExpanded(False)
                    except Exception:
                        pass
                    # reload children; _load_tables_for_connection handles errors internally
                    self._load_tables_for_connection(node, conn_name)
                    try:
                        node.setExpanded(True)
                    except Exception:
                        pass
                    try:
                        self.statusBar().showMessage(f"Refreshed tables for '{conn_name}'", 3000)
                    except Exception:
                        pass
                except Exception as e:
                    try:
                        QMessageBox.warning(self, "Refresh failed", f"Failed to refresh tables for '{conn_name}': {e}")
                    except Exception:
                        pass
            elif action == edit_act:
                self._edit_connection_item(conn_name, item)
            elif action == delete_act:
                self._delete_connection_item(conn_name, item)
        except Exception:
            # best-effort: ignore UI errors
            pass

    def _edit_connection_item(self, connection_name: str, tree_item: QTreeWidgetItem | None = None):
        """Allow editing a stored connection. Presents the ConnectionDialog pre-filled from saved config.

        Implementation strategy: backup existing in-memory engine/config, remove the existing connection, try to add the new one with the dialog data. On failure, restore the previous config/engine.
        """
        try:
            # attempt to fetch existing config (best-effort)
            orig_cfg = None
            orig_engine = None
            if connection_name in self.conn_mgr._configs:
                try:
                    orig_cfg = self.conn_mgr._configs.get(connection_name).copy()
                except Exception:
                    orig_cfg = self.conn_mgr._configs.get(connection_name)
            if connection_name in self.conn_mgr._engines:
                orig_engine = self.conn_mgr._engines.get(connection_name)

            dlg = ConnectionDialog(self)
            # Prefill dialog fields from existing config where possible
            if orig_cfg:
                try:
                    dtype = orig_cfg.get('type') or 'postgresql'
                    dlg.type_combo.setCurrentText(dtype)
                    dlg.name_edit.setText(connection_name)
                    # sqlite stored configs use 'path'
                    if dtype == 'sqlite':
                        path = orig_cfg.get('path') or (orig_cfg.get('url') or '').replace('sqlite:///', '')
                        dlg.db_edit.setText(path)
                    else:
                        # host/port/user/database/schema keys may be present in cfg or under params
                        dlg.host_edit.setText(str(orig_cfg.get('host') or orig_cfg.get('host', '') or ''))
                        # port may be stored as string or number
                        p = orig_cfg.get('port') or (orig_cfg.get('params', {}).get('port') if isinstance(orig_cfg.get('params', {}), dict) else orig_cfg.get('port'))
                        if p:
                            dlg.port_edit.setText(str(p))
                        # the connection manager historically stores 'user' key
                        dlg.user_edit.setText(str(orig_cfg.get('user') or orig_cfg.get('username') or ''))
                        dlg.db_edit.setText(str(orig_cfg.get('database') or orig_cfg.get('db') or ''))
                        dlg.schema_edit.setText(str(orig_cfg.get('schema') or ''))
                        # Preserve the stored password so editing doesn't clear it unintentionally.
                        try:
                            pwd = orig_cfg.get('password') if isinstance(orig_cfg, dict) else None
                            if not pwd:
                                # some configs may store under 'params' or 'credentials'
                                pwd = (orig_cfg.get('params') or {}).get('password') if isinstance(orig_cfg.get('params', {}), dict) else pwd
                            dlg.password_edit.setText(str(pwd or ''))
                        except Exception:
                            pass
                except Exception:
                    pass

            if dlg.exec() != QDialog.DialogCode.Accepted:
                return
            data = dlg.get_data()

            # Merge omitted fields from original config so blanks in the dialog don't wipe values
            if orig_cfg:
                try:
                    # preserve password if dialog left it empty
                    if not data.get('password') and orig_cfg.get('password'):
                        data['password'] = orig_cfg.get('password')
                    # preserve schema/search_path if left empty
                    if not data.get('schema'):
                        data['schema'] = orig_cfg.get('schema') or (orig_cfg.get('params') or {}).get('schema')
                    # preserve other common page inputs when left blank
                    for k in ('host', 'port', 'username', 'user', 'database'):
                        if not data.get(k) and orig_cfg.get(k):
                            data[k] = orig_cfg.get(k)
                except Exception:
                    pass

            # Add new connection first. Do NOT remove the old connection until the new one is successfully created.
            new_name = None
            try:
                if data.get('type') == 'sqlite':
                    # database key for sqlite contains path
                    new_name = self.conn_mgr.add_sqlite_connection(data.get('database'))
                else:
                    # map dialog 'username' to 'user' expected by add_connection
                    user_val = data.get('username') or data.get('user')
                    logger.debug(f"Editing connection with parameters: {data}")
                    new_name = self.conn_mgr.add_connection(
                        data.get('name') or connection_name,
                        data.get('type'),
                        host=data.get('host'),
                        port=data.get('port'),
                        user=user_val,
                        password=data.get('password'),
                        database=data.get('database'),
                        jdbc=data.get('jdbc'),
                        schema=data.get('schema'),
                    )
            except Exception as e:
                # No changes were made to existing configs/engines, so just report the error
                QMessageBox.critical(self, "Edit failed", f"Failed to apply edited connection: {e}")
                return

            # If we successfully added the new connection and the display name changed, remove the old one
            try:
                if new_name and new_name != connection_name:
                    try:
                        self.conn_mgr.remove_connection(connection_name)
                    except Exception:
                        pass
            except Exception:
                pass

            # Update UI: remove old top-level item and add the new one
            try:
                if tree_item is not None:
                    idx = self.conn_tree.indexOfTopLevelItem(tree_item)
                    if idx >= 0:
                        self.conn_tree.takeTopLevelItem(idx)
                # Add new item and expand
                self._add_connection_item(new_name)
                # Ensure engine metadata is fresh and reload the table list using the new credentials
                try:
                    eng = None
                    try:
                        eng = self.conn_mgr.get_connection(new_name)
                    except Exception:
                        eng = None
                    if eng is not None:
                        try:
                            clear_schema_cache(eng)
                        except Exception:
                            pass
                        try:
                            item = self.find_connection_item(new_name)
                            if item:
                                self._load_tables_for_connection(item, new_name)
                        except Exception:
                            pass
                except Exception:
                    pass
                item = self.find_connection_item(new_name)
                if item:
                    item.setExpanded(True)
                    self.conn_tree.setCurrentItem(item)
                # Refresh connection selectors in open SQL tabs so users can pick the updated connection
                try:
                    self._refresh_connection_combos()
                except Exception:
                    pass
                QMessageBox.information(self, "Connection updated", f"Connection updated: {new_name}")
            except Exception:
                pass

        except Exception as e:
            QMessageBox.critical(self, "Edit error", str(e))

    def _delete_connection_item(self, connection_name: str, tree_item: QTreeWidgetItem | None = None):
        """Delete a stored connection after user confirmation and remove it from the tree."""
        try:
            resp = QMessageBox.question(self, "Delete connection", f"Are you sure you want to delete connection '{connection_name}'?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if resp != QMessageBox.StandardButton.Yes:
                return
            try:
                self.conn_mgr.remove_connection(connection_name)
            except KeyError:
                # already absent
                pass
            except Exception as e:
                QMessageBox.warning(self, "Delete failed", f"Failed to remove connection: {e}")
                return
            # remove from UI
            try:
                if tree_item is None:
                    tree_item = self.find_connection_item(connection_name)
                if tree_item is not None:
                    idx = self.conn_tree.indexOfTopLevelItem(tree_item)
                    if idx >= 0:
                        self.conn_tree.takeTopLevelItem(idx)
                # Refresh connection selectors in open SQL tabs so removed connection isn't selectable
                try:
                    self._refresh_connection_combos()
                except Exception:
                    pass
                QMessageBox.information(self, "Deleted", f"Connection '{connection_name}' deleted.")
            except Exception:
                pass
        except Exception as e:
            QMessageBox.critical(self, "Delete error", str(e))

    def _on_editor_tab_close_requested(self, index: int):
        try:
            if index < 0 or index >= self.editor_tabs.count():
                return
            container = self.editor_tabs.widget(index)
            if container is None:
                self.editor_tabs.removeTab(index)
                return
            # If a worker is running, ask for confirmation to cancel
            worker = getattr(container, '_worker', None)
            if worker is not None:
                try:
                    running = getattr(worker, 'isRunning', None)
                    if callable(running):
                        is_running = worker.isRunning()
                    else:
                        is_running = True
                except Exception:
                    is_running = True
                if is_running:
                    resp = QMessageBox.question(self, "Close tab", "A query is running in this tab. Cancel and close?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                    if resp != QMessageBox.StandardButton.Yes:
                        return
                    try:
                        worker.stop()
                    except Exception:
                        pass
            # finally remove the tab
            self.editor_tabs.removeTab(index)
        except Exception:
            # best-effort: ignore UI errors
            pass

    def _refresh_connection_combos(self):
        """Refresh the connection dropdowns in all open SQL editor tabs.

        Preserves the current selection where possible.
        """
        try:
            names = list(self.conn_mgr.list_connections())
        except Exception:
            names = []

        for i in range(self.editor_tabs.count()):
            container = self.editor_tabs.widget(i)
            if container is None:
                continue
            combo = getattr(container, '_conn_combo', None)
            if combo is None:
                continue
            try:
                cur = combo.currentText() or ""
            except Exception:
                cur = ""
            try:
                combo.blockSignals(True)
                combo.clear()
                combo.addItem("")
                for n in names:
                    combo.addItem(n)
                # Restore previous selection if still available
                if cur:
                    idx = combo.findText(cur)
                    if idx >= 0:
                        combo.setCurrentIndex(idx)
                    else:
                        # try to select currently selected connection in tree
                        sel = self._selected_connection_name()
                        if sel:
                            idx2 = combo.findText(sel)
                            if idx2 >= 0:
                                combo.setCurrentIndex(idx2)
                else:
                    sel = self._selected_connection_name()
                    if sel:
                        idx2 = combo.findText(sel)
                        if idx2 >= 0:
                            combo.setCurrentIndex(idx2)
            finally:
                try:
                    combo.blockSignals(False)
                except Exception:
                    pass
        return

    def _on_tab_connection_changed(self, container, combo):
        """Handler invoked when a tab's connection combobox selection changes.

        Best-effort: apply saved schema/search_path for PostgreSQL connections by
        executing a lightweight SET search_path on a fresh connection. For MySQL,
        attempt a USE <database> when appropriate. Then refresh the left-side
        tables for the selected connection so the UI shows the schema-specific tables.
        """
        if combo is None:
            return
        try:
            conn_name = combo.currentText().strip()
            if not conn_name:
                return
            # try to get engine and saved config
            try:
                engine = self.conn_mgr.get_connection(conn_name)
            except Exception:
                engine = None
            cfg = getattr(self.conn_mgr, '_configs', {}).get(conn_name, {}) if conn_name else {}
            schema = None
            if isinstance(cfg, dict):
                schema = cfg.get('schema') or (cfg.get('params') or {}).get('schema')

            # Determine dialect name safely
            dialect_name = None
            try:
                dialect = getattr(engine, 'dialect', None)
                dialect_name = getattr(dialect, 'name', None)
            except Exception:
                dialect_name = None

            # Apply session-level schema where applicable
            try:
                if engine is not None and schema and dialect_name == 'postgresql':
                    try:
                        with engine.connect() as conn:
                            conn.exec_driver_sql(f"SET search_path TO {schema!r}")
                        # Invalidate any cached schema for this engine so UI/AI use fresh metadata
                        try:
                            clear_schema_cache(engine)
                        except Exception:
                            pass
                    except Exception:
                        # best-effort; ignore failures
                        pass
                elif engine is not None and dialect_name in ('mysql',) and isinstance(cfg.get('database'), str):
                    try:
                        with engine.connect() as conn:
                            conn.exec_driver_sql(f"USE `{cfg.get('database')}`")
                        try:
                            clear_schema_cache(engine)
                        except Exception:
                            pass
                    except Exception:
                        pass
            except Exception:
                pass

            # Refresh left-tree table listing for this connection so schema change is visible
            try:
                item = self.find_connection_item(conn_name)
                if item:
                    self._load_tables_for_connection(item, conn_name)
            except Exception:
                pass
        except Exception:
            pass

    def _restore_app_state(self):
        """Restore simple app state on startup: populate first SQL tab editor with saved SQL if available."""
        try:
            state = load_app_state() or {}
            last_sql = state.get('last_sql') if isinstance(state, dict) else None
            if last_sql:
                # ensure at least one tab exists
                if self.editor_tabs.count() == 0:
                    self.new_sql_tab()
                # take first tab
                container = self.editor_tabs.widget(0)
                if container is not None:
                    editor = getattr(container, '_editor', None)
                    if editor is None:
                        editor = container.findChild(SqlEditor)
                    if editor is not None:
                        try:
                            editor.set_sql(str(last_sql))
                        except Exception:
                            pass
        except Exception:
            # best-effort: do not block startup
            pass

    def closeEvent(self, event):
        """Save simple app state (first tab SQL content) on application exit."""
        try:
            # grab first tab editor content
            last_sql = None
            try:
                if self.editor_tabs.count() > 0:
                    container = self.editor_tabs.widget(0)
                    if container is not None:
                        editor = getattr(container, '_editor', None)
                        if editor is None:
                            editor = container.findChild(SqlEditor)
                        if editor is not None:
                            try:
                                last_sql = editor.get_sql()
                            except Exception:
                                last_sql = None
            except Exception:
                last_sql = None

            state = {}
            if last_sql:
                state['last_sql'] = last_sql
            # persist dark mode preference if available
            try:
                if getattr(self, '_dark_mode_enabled', False):
                    state['dark_mode'] = True
            except Exception:
                pass
            try:
                save_app_state(state)
            except Exception:
                # swallow save errors to not block exit
                pass
        except Exception:
            pass
        # proceed with normal close
        try:
            super().closeEvent(event)
        except Exception:
            event.accept()