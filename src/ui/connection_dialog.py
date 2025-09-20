from PyQt6.QtWidgets import QDialog, QVBoxLayout, QFormLayout, QLineEdit, QComboBox, QDialogButtonBox, QLabel, QPushButton, QMessageBox, QFileDialog, QStyle
from PyQt6.QtCore import Qt

from db.connection import parse_jdbc_url
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text

class ConnectionDialog(QDialog):
    """Dialog to input a new DB connection (mysql, postgresql, or sqlite)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("New Connection")
        self.resize(500, 240)

        layout = QVBoxLayout(self)
        form = QFormLayout()

        # JDBC URL input (optional)
        self.jdbc_edit = QLineEdit()
        parse_btn = QPushButton("Parse JDBC")
        parse_btn.setToolTip("Paste a JDBC URL and click to auto-fill fields")
        jdbc_hbox = QVBoxLayout()
        # put line edit and button on same line using a temporary layout widget
        from PyQt6.QtWidgets import QWidget, QHBoxLayout
        h = QWidget()
        hbox = QHBoxLayout(h)
        hbox.setContentsMargins(0,0,0,0)
        hbox.addWidget(self.jdbc_edit)
        hbox.addWidget(parse_btn)
        form.addRow("JDBC URL (optional):", h)

        self.name_edit = QLineEdit()
        form.addRow("Connection name:", self.name_edit)

        self.type_combo = QComboBox()
        # include sqlite as a simple file-based option
        self.type_combo.addItems(["postgresql", "mysql", "sqlite"])
        self.type_combo.currentTextChanged.connect(self._on_type_changed)
        form.addRow("DB Type:", self.type_combo)

        self.host_edit = QLineEdit("localhost")
        form.addRow("Host:", self.host_edit)

        self.port_edit = QLineEdit()
        self.port_edit.setPlaceholderText("Leave blank for default port")
        form.addRow("Port:", self.port_edit)

        self.user_edit = QLineEdit()
        form.addRow("User:", self.user_edit)

        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("Password:", self.password_edit)

        # For sqlite we allow browsing for a file. Browse button is enabled only for sqlite type.
        self.db_edit = QLineEdit()
        self.db_edit.setPlaceholderText("Database name or path (for sqlite select file)")
        db_h = QWidget()
        db_hbox = QHBoxLayout(db_h)
        db_hbox.setContentsMargins(0, 0, 0, 0)
        self.db_edit.setMinimumWidth(220)
        db_hbox.addWidget(self.db_edit)
        self._browse_db_btn = QPushButton("Browse")
        self._browse_db_btn.setToolTip("Select sqlite database file")
        try:
            self._browse_db_btn.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DirOpenIcon))
        except Exception:
            pass
        self._browse_db_btn.setEnabled(False)
        self._browse_db_btn.clicked.connect(self._on_browse_sqlite)
        db_hbox.addWidget(self._browse_db_btn)
        form.addRow("Database:", db_h)

        # Optional schema/search_path field (mainly for PostgreSQL)
        self.schema_edit = QLineEdit()
        self.schema_edit.setPlaceholderText("Optional schema / search_path (PostgreSQL)")
        form.addRow("Schema (Postgres):", self.schema_edit)

        layout.addLayout(form)

        self.note = QLabel("Note: Password is stored in plain config. Consider enabling encryption later.")
        self.note.setWordWrap(True)
        layout.addWidget(self.note)

        # Test connection button — attempts a live connect without saving configuration
        self._test_btn = QPushButton("Test Connection")
        self._test_btn.setToolTip("Attempt to connect using the values above without saving the connection")
        try:
            self._test_btn.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogYesButton))
        except Exception:
            pass
        self._test_btn.clicked.connect(self._on_test_connection)
        layout.addWidget(self._test_btn)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        # set icons for dialog buttons to improve discoverability
        try:
            ok_btn = buttons.button(QDialogButtonBox.StandardButton.Ok)
            cancel_btn = buttons.button(QDialogButtonBox.StandardButton.Cancel)
            ok_btn.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton))
            cancel_btn.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogCancelButton))
        except Exception:
            pass
        layout.addWidget(buttons)

        # Wire parse button
        try:
            parse_btn.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogOpenButton))
        except Exception:
            pass
        parse_btn.clicked.connect(self._on_parse_jdbc)

        # initialize fields according to selected type
        self._on_type_changed(self.type_combo.currentText())

    def _on_test_connection(self):
        """Validate the dialog inputs and attempt a quick connection. Does not persist configuration.

        This is a best-effort test: it will try to build a SQLAlchemy engine and open a short-lived
        connection. Any errors are presented to the user.
        """
        try:
            data = self.get_data()
        except Exception as e:
            QMessageBox.critical(self, "Validation failed", f"Invalid input: {e}")
            return

        try:
            # If a JDBC URL is supplied, prefer parsing it
            jdbc = data.get('jdbc')
            if jdbc:
                parsed = parse_jdbc_url(jdbc)
                conn_type = parsed.get('conn_type')
                host = parsed.get('host')
                port = parsed.get('port')
                user = parsed.get('username')
                password = parsed.get('password')
                database = parsed.get('database')
                params = parsed.get('params') or {}
            else:
                conn_type = data.get('type')
                host = data.get('host')
                port = data.get('port')
                user = data.get('username')
                password = data.get('password')
                database = data.get('database')
                params = {}

            if conn_type == 'sqlite':
                if not os.path.exists(database):
                    raise FileNotFoundError(f"SQLite file not found: {database}")
                url = f"sqlite:///{os.path.abspath(database)}"
                engine = create_engine(url, future=True)
            else:
                if conn_type == 'postgresql':
                    driver = 'psycopg2'
                    drivername = f"postgresql+{driver}"
                    default_port = 5432
                elif conn_type == 'mysql':
                    driver = 'pymysql'
                    drivername = f"mysql+{driver}"
                    default_port = 3306
                else:
                    raise ValueError(f"Unsupported DB type for test: {conn_type}")

                qparams = {}
                for k, v in (params or {}).items():
                    if v is None:
                        continue
                    qparams[k] = str(v)

                url_obj = URL.create(
                    drivername=drivername,
                    username=user or None,
                    password=password or None,
                    host=host or None,
                    port=int(port) if port else default_port,
                    database=database or None,
                    query=qparams or None,
                )
                engine = create_engine(url_obj, future=True)

            # Try a short-lived connect
            try:
                with engine.connect() as conn:
                    # For some drivers a simple execution verifies auth/handshake
                    conn.execute(text("SELECT 1"))
            finally:
                try:
                    engine.dispose()
                except Exception:
                    pass

            QMessageBox.information(self, "Success", "Successfully connected to the database.")
        except Exception as e:
            QMessageBox.critical(self, "Connection failed", f"Failed to connect: {e}")
            return

    def _on_parse_jdbc(self):
        txt = self.jdbc_edit.text().strip()
        if not txt:
            QMessageBox.information(self, "Parse JDBC", "Please paste a JDBC URL first.")
            return
        try:
            parsed = parse_jdbc_url(txt)
        except Exception as e:
            QMessageBox.critical(self, "Parse error", f"Failed to parse JDBC URL: {e}")
            return
        # Fill fields where applicable
        conn_type = parsed.get('conn_type')
        if conn_type:
            idx = self.type_combo.findText(conn_type)
            if idx >= 0:
                self.type_combo.setCurrentIndex(idx)
        host = parsed.get('host')
        if host:
            self.host_edit.setText(str(host))
        port = parsed.get('port')
        if port:
            self.port_edit.setText(str(port))
        db = parsed.get('database')
        if db:
            self.db_edit.setText(str(db))
        # some JDBC strings or connection URIs may include a schema/search_path
        schema = parsed.get('schema')
        if schema:
            self.schema_edit.setText(str(schema))
        user = parsed.get('username')
        if user:
            self.user_edit.setText(str(user))
        pwd = parsed.get('password')
        if pwd:
            self.password_edit.setText(str(pwd))
        QMessageBox.information(self, "Parsed", "JDBC URL parsed and fields updated.")

    def _on_browse_sqlite(self):
        """Open a file dialog to select a sqlite database and place the path into the database field."""
        try:
            path, _ = QFileDialog.getOpenFileName(self, "Select SQLite database file", "", "SQLite Files (*.db *.sqlite);;All Files (*)")
            if path:
                self.db_edit.setText(path)
        except Exception:
            # best-effort: ignore UI errors
            pass

    def _on_type_changed(self, t: str):
        """Enable/disable fields according to the selected database type."""
        t = (t or "").lower()
        is_sqlite = (t == 'sqlite')
        is_postgres = (t == 'postgresql')
        # For sqlite only the database/path field is relevant
        self.host_edit.setEnabled(not is_sqlite)
        self.port_edit.setEnabled(not is_sqlite)
        self.user_edit.setEnabled(not is_sqlite)
        self.password_edit.setEnabled(not is_sqlite)
        self._browse_db_btn.setEnabled(is_sqlite)
        # schema only relevant for PostgreSQL; otherwise keep it disabled
        self.schema_edit.setEnabled(is_postgres)
        if is_sqlite:
            self.db_edit.setPlaceholderText("Select sqlite database file")
        else:
            self.db_edit.setPlaceholderText("Database name or schema")

    def get_data(self) -> dict:
        """Return the dialog data as a dictionary suitable for ConnectionManager.add_connection.

        The returned dict contains keys:
          - name: connection name (str)
          - type: 'postgresql'|'mysql'|'sqlite' (str)
          - host: host or None
          - port: int or None
          - username: str or None
          - password: str or None
          - database: database name or sqlite file path
          - schema: optional schema/search_path (Postgres)
          - jdbc: raw JDBC URL if provided

        Basic validation is performed and ValueError is raised for missing required
        fields (e.g. name or sqlite file path).
        """
        t = (self.type_combo.currentText() or "").lower()
        name = self.name_edit.text().strip()
        jdbc = self.jdbc_edit.text().strip() or None

        port_txt = self.port_edit.text().strip()
        port = None
        if port_txt:
            try:
                port = int(port_txt)
            except ValueError:
                # Let callers/UI handle validation; raise a clear error
                raise ValueError(f"Port must be an integer, got: {port_txt}")
            if port <= 0 or port > 65535:
                raise ValueError(f"Port out of valid range: {port}")

        # For sqlite many fields are irrelevant
        if t == 'sqlite':
            host = None
            username = None
            password = None
            database = self.db_edit.text().strip() or None
        else:
            host = self.host_edit.text().strip() or None
            username = self.user_edit.text().strip() or None
            password = self.password_edit.text().strip() or None
            database = self.db_edit.text().strip() or None

        # Basic required-field validation
        if not name:
            raise ValueError("Connection name is required")

        if t == 'sqlite':
            if not database:
                raise ValueError("For sqlite, a database file path is required")
        else:
            # For networked DBs require host and database name
            if not host:
                raise ValueError("Host is required for non-sqlite connections")
            if not database:
                raise ValueError("Database name is required for this connection type")

        # schema only applicable to Postgres
        schema = None
        if t == 'postgresql':
            s = self.schema_edit.text().strip()
            schema = s if s else None

        return {
            'name': name,
            'type': t,
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'database': database,
            'schema': schema,
            'jdbc': jdbc,
        }

    def accept(self) -> None:
        """Validate inputs and accept the dialog. Validation is lightweight: ensure a type and either host/database as appropriate."""
        dtype = self.type_combo.currentText().strip()
        if not dtype:
            QMessageBox.warning(self, "Missing type", "Please select a database type.")
            return
        # Basic validation for sqlite vs server types
        if dtype == 'sqlite':
            if not self.db_edit.text().strip():
                QMessageBox.warning(self, "Missing database", "Please select a sqlite database file.")
                return
        else:
            if not self.host_edit.text().strip():
                QMessageBox.warning(self, "Missing host", "Please enter a host for the connection.")
                return
        super().accept()
