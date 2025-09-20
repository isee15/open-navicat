from PyQt6.QtWidgets import QDialog, QHBoxLayout, QPlainTextEdit, QVBoxLayout, QLabel, QTreeWidget, QTreeWidgetItem, QMenu, QMessageBox
from PyQt6.QtCore import Qt, pyqtSignal, QPoint
from PyQt6.QtGui import QGuiApplication
from sqlalchemy import inspect
from db.metadata import get_create_sql_for_table, get_create_sql_for_connection

class SchemaViewerDialog(QDialog):
    """Dialog to view schema of a database connection (tables and details).

    Emits table_activated(table_name) when the user double-clicks a table item in the tree.
    """

    table_activated = pyqtSignal(str)

    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Schema Viewer")
        self.resize(900, 600)
        self.engine = engine

        layout = QHBoxLayout(self)

        # Use a tree so we can later show other object types (views, indexes, etc.)
        self.tree = QTreeWidget()
        self.tree.setHeaderHidden(True)
        self.tree.setMaximumWidth(320)
        layout.addWidget(self.tree)

        right_layout = QVBoxLayout()
        self.info_label = QLabel("Select a table to view details")
        right_layout.addWidget(self.info_label)
        self.detail = QPlainTextEdit()
        self.detail.setReadOnly(True)
        right_layout.addWidget(self.detail)

        layout.addLayout(right_layout)

        self._load_items()
        self.tree.itemDoubleClicked.connect(self._on_item_double_clicked)
        self.tree.currentItemChanged.connect(self._on_item_selected)

        # enable custom context menu on the tree for copy-create-sql actions
        self.tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self._on_context_menu)

    def _load_items(self):
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            tables.sort()
            # top-level 'Tables'
            self.tree.clear()
            tables_root = QTreeWidgetItem(self.tree, ["Tables"])
            tables_root.setExpanded(True)
            for t in tables:
                it = QTreeWidgetItem(tables_root, [t])
                it.setData(0, Qt.ItemDataRole.UserRole, ("table", t))
            # future: add views, indexes, etc.
        except Exception as e:
            self.info_label.setText(f"Failed to load tables: {e}")

    def _on_item_selected(self, current, previous):
        if current is None:
            return
        data = current.data(0, Qt.ItemDataRole.UserRole)
        if data and data[0] == 'table':
            table_name = data[1]
            self._show_table_details(table_name)
        else:
            # clear details for non-table nodes
            self.detail.clear()
            self.info_label.setText("Select a table to view details")

    def _on_item_double_clicked(self, item, column):
        data = item.data(0, Qt.ItemDataRole.UserRole)
        if data and data[0] == 'table':
            table_name = data[1]
            # emit signal for external handlers (e.g., main window to run a query)
            self.table_activated.emit(table_name)

    def _on_context_menu(self, pos: QPoint):
        """Show context menu for tables root or individual table to copy CREATE SQL to clipboard."""
        try:
            item = self.tree.itemAt(pos)
            if item is None:
                return

            data = item.data(0, Qt.ItemDataRole.UserRole)
            menu = QMenu(self)

            # If this is a table node
            if data and data[0] == 'table':
                table_name = data[1]
                act = menu.addAction("Copy CREATE SQL")
                def _copy_table():
                    try:
                        ddl = get_create_sql_for_table(self.engine, table_name)
                        if not ddl:
                            QMessageBox.warning(self, "No DDL", f"Could not obtain CREATE statement for table: {table_name}")
                            return
                        QGuiApplication.clipboard().setText(ddl)
                        QMessageBox.information(self, "Copied", f"CREATE SQL for table '{table_name}' copied to clipboard.")
                    except Exception as e:
                        QMessageBox.critical(self, "Error", f"Failed to copy CREATE SQL: {e}")

                act.triggered.connect(_copy_table)

            else:
                # Possibly the root 'Tables' node - allow copying all tables' CREATE statements
                text = item.text(0) if item.text(0) is not None else ""
                if text.lower().startswith('tables'):
                    act = menu.addAction("Copy CREATE SQL (all tables)")
                    def _copy_all():
                        try:
                            ddl = get_create_sql_for_connection(self.engine)
                            if not ddl:
                                QMessageBox.warning(self, "No DDL", "Could not obtain CREATE statements for this connection.")
                                return
                            QGuiApplication.clipboard().setText(ddl)
                            QMessageBox.information(self, "Copied", "CREATE SQL for all tables copied to clipboard.")
                        except Exception as e:
                            QMessageBox.critical(self, "Error", f"Failed to copy CREATE SQL: {e}")

                    act.triggered.connect(_copy_all)

            # show menu if it has actions
            if menu.actions():
                global_pos = self.tree.viewport().mapToGlobal(pos)
                menu.exec(global_pos)
        except Exception:
            # best-effort: swallow UI errors
            return

    def _show_table_details(self, table_name: str):
        if not table_name:
            return
        try:
            inspector = inspect(self.engine)
            cols = inspector.get_columns(table_name)
            pk = inspector.get_pk_constraint(table_name)
            fks = inspector.get_foreign_keys(table_name)
            idxs = inspector.get_indexes(table_name)

            lines = []
            lines.append(f"Table: {table_name}\n")
            lines.append("Columns:")
            for c in cols:
                col_line = f" - {c.get('name')} : {c.get('type')}{' NULL' if c.get('nullable') else ' NOT NULL'}"
                if c.get('default') is not None:
                    col_line += f" DEFAULT {c.get('default')}"
                lines.append(col_line)

            lines.append("\nPrimary Key:")
            lines.append(str(pk.get('constrained_columns')))

            lines.append("\nForeign Keys:")
            if fks:
                for fk in fks:
                    lines.append(f" - columns={fk.get('constrained_columns')} references={fk.get('referred_table')}({fk.get('referred_columns')})")
            else:
                lines.append(" - (none)")

            lines.append("\nIndexes:")
            if idxs:
                for ix in idxs:
                    lines.append(f" - {ix.get('name')} columns={ix.get('column_names')} unique={ix.get('unique')}")
            else:
                lines.append(" - (none)")

            # Try to fetch create statement for sqlite
            dialect = self.engine.dialect.name.lower()
            create_sql = None
            if dialect == 'sqlite':
                try:
                    with self.engine.connect() as conn:
                        res = conn.exec_driver_sql("SELECT sql FROM sqlite_master WHERE type='table' AND name=:n", {'n': table_name})
                        row = res.fetchone()
                        if row and row[0]:
                            create_sql = row[0]
                except Exception:
                    create_sql = None
            elif dialect == 'mysql':
                try:
                    with self.engine.connect() as conn:
                        res = conn.exec_driver_sql(f"SHOW CREATE TABLE `{table_name}`")
                        row = res.fetchone()
                        if row and len(row) >= 2:
                            create_sql = row[1]
                except Exception:
                    create_sql = None

            if create_sql:
                lines.append('\nCreate Statement:\n')
                lines.append(create_sql)

            self.detail.setPlainText('\n'.join(lines))
            self.info_label.setText(f"Table: {table_name}")
        except Exception as e:
            self.detail.setPlainText(f"Failed to load details: {e}")
            self.info_label.setText("Error")
