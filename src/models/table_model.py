from typing import List, Tuple, Any, Optional, Dict
from PyQt6.QtCore import QAbstractTableModel, Qt, QModelIndex
from PyQt6.QtGui import QBrush, QColor

class TableModel(QAbstractTableModel):
    """Simple table model for query results with optional editing support.

    columns: list of column names
    rows: list of row tuples

    If pk_columns is provided (list of column names), the model will allow editing
    of non-PK columns, track pending edits, and support row removal. Edits are kept
    in-memory until the UI calls get_pending_changes()/get_pending_deletes() and
    applies them to the database.
    """

    def __init__(self, columns: List[str], rows: List[Tuple[Any, ...]], pk_columns: Optional[List[str]] = None):
        super().__init__()
        self._columns = columns
        # store rows as mutable lists for easy editing while preserving original tuple shape when exporting
        self._rows = [list(r) for r in rows]
        # keep a copy of original values to help generate WHERE clauses from original PKs if needed
        self._original_rows = [list(r) for r in rows]
        # map column name -> index for convenience
        self._col_index: Dict[str, int] = {c: i for i, c in enumerate(columns)}
        # primary key column names (optional). If provided, PK columns will be treated as non-editable
        self._pk_columns = pk_columns or []
        # track edits: row_index -> {col_index: new_value}
        self._edits: Dict[int, Dict[int, Any]] = {}
        # store PK values for rows removed (so deletes can be applied after row removal)
        self._deleted_pk_values: List[Dict[str, Any]] = []

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._columns)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        try:
            value = self._rows[index.row()][index.column()]
        except Exception:
            return None

        # For display, return a user-friendly string. For editing, return the raw value
        # so delegates/editors can operate on the underlying Python object.
        if role == Qt.ItemDataRole.DisplayRole:
            return "" if value is None else str(value)
        if role == Qt.ItemDataRole.EditRole:
            return value
        # Highlight cells with pending edits using an orange background
        if role == Qt.ItemDataRole.BackgroundRole:
            try:
                r = index.row()
                c = index.column()
                if r in self._edits and c in self._edits.get(r, {}):
                    return QBrush(QColor(255, 165, 0, 120))
            except Exception:
                pass
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            if 0 <= section < len(self._columns):
                return self._columns[section]
            return None
        else:
            return section + 1

    def flags(self, index: QModelIndex):
        # Return combined Qt.ItemFlag values directly to avoid referencing Qt.ItemFlags
        if not index.isValid():
            # return an empty flag set (0) — using ItemFlag(0) produces a zero-valued flag
            try:
                return Qt.ItemFlag(0)
            except Exception:
                # fallback to a minimal flag if the zero value isn't constructible
                return Qt.ItemFlag.ItemIsEnabled
        # Base flags: selectable and enabled
        flags = Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled
        # If PK columns are known, treat PK columns as read-only and allow editing for others
        colname = self._columns[index.column()]
        if self._pk_columns:
            if colname in self._pk_columns:
                return flags
            return flags | Qt.ItemFlag.ItemIsEditable

        # If no PK info, allow inline editing so users can edit values in the grid.
        return flags | Qt.ItemFlag.ItemIsEditable

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.ItemDataRole.EditRole) -> bool:
        """Set an edited value in the model. If the new value equals the original
        value (by display string), remove any pending edit for that cell. Otherwise
        record the pending edit. Ensure BackgroundRole updates are emitted so the
        view can show/remove the orange highlight.
        """
        if not index.isValid():
            return False
        if role != Qt.ItemDataRole.EditRole:
            return False
        try:
            r = index.row()
            c = index.column()
            # ensure not editing a PK column
            colname = self._columns[c]
            if colname in self._pk_columns:
                return False

            new_val = value

            # Determine the original value (fall back to current visible value if necessary)
            orig = None
            try:
                orig = self._original_rows[r][c]
            except Exception:
                try:
                    orig = self._rows[r][c]
                except Exception:
                    orig = None

            # Compare on display string to avoid spurious edits due to type differences
            try:
                disp_orig = "" if orig is None else str(orig)
            except Exception:
                disp_orig = ""
            try:
                disp_new = "" if new_val is None else str(new_val)
            except Exception:
                disp_new = ""

            if disp_orig == disp_new:
                # No meaningful change — remove any existing pending edit for this cell
                try:
                    if r in self._edits and c in self._edits.get(r, {}):
                        del self._edits[r][c]
                        if not self._edits[r]:
                            del self._edits[r]
                except Exception:
                    pass
                # restore visible value to original to keep model consistent
                try:
                    self._rows[r][c] = orig
                except Exception:
                    pass
                topLeft = self.index(r, c)
                self.dataChanged.emit(topLeft, topLeft, [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole, Qt.ItemDataRole.BackgroundRole])
                return True

            # record edit
            self._edits.setdefault(r, {})[c] = new_val
            # apply to visible rows
            self._rows[r][c] = new_val
            topLeft = self.index(r, c)
            self.dataChanged.emit(topLeft, topLeft, [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole, Qt.ItemDataRole.BackgroundRole])
            return True
        except Exception:
            return False

    def get_all_data(self) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """Return columns and rows for export. Rows are returned as tuples.

        This reflects current in-memory values (including edits and removals).
        """
        rows = [tuple(r) for r in self._rows]
        return self._columns, rows

    # --- Editing utilities ---
    def has_pending_changes(self) -> bool:
        return bool(self._edits) or bool(self._deleted_pk_values)

    def get_pending_changes(self) -> List[Dict[str, Any]]:
        """Return a list of pending updates in the format:
        [{'row': int, 'pk': {col: value, ...}, 'changes': {col: value, ...}}, ...]

        PK values are taken from current row values for the configured PK columns.
        """
        out: List[Dict[str, Any]] = []
        if not self._pk_columns:
            return out
        for r, changes in self._edits.items():
            # only include rows that still exist
            try:
                pk = {pkc: self._rows[r][self._col_index[pkc]] for pkc in self._pk_columns}
            except Exception:
                pk = {}
            ch = {self._columns[c]: self._rows[r][c] for c in changes.keys()}
            out.append({'row': r, 'pk': pk, 'changes': ch})
        return out

    def get_pending_deletes(self) -> List[Dict[str, Any]]:
        """Return a list of PK dicts for rows marked for deletion."""
        # return the preserved PK values captured at deletion time
        return list(self._deleted_pk_values)

    def mark_row_deleted(self, row: int) -> None:
        """Mark a row as deleted and remove it from the model (emits beginRemoveRows/endRemoveRows).

        The original PK values are preserved in _deleted_pk_values so callers can issue DELETE statements.
        """
        if row < 0 or row >= len(self._rows):
            return
        self.beginRemoveRows(QModelIndex(), row, row)
        try:
            # capture original PK values from _original_rows (fallback to current row if absent)
            pk_vals: Dict[str, Any] = {}
            if self._pk_columns:
                try:
                    pk_vals = {pkc: self._original_rows[row][self._col_index[pkc]] for pkc in self._pk_columns}
                except Exception:
                    try:
                        pk_vals = {pkc: self._rows[row][self._col_index[pkc]] for pkc in self._pk_columns}
                    except Exception:
                        pk_vals = {}
            if pk_vals:
                self._deleted_pk_values.append(pk_vals)

            # physically remove from lists
            del self._rows[row]
            del self._original_rows[row]

            # Need to rebuild edits mapping indices: shift indices greater than removed row
            new_edits: Dict[int, Dict[int, Any]] = {}
            for r_idx, ed in self._edits.items():
                if r_idx == row:
                    continue
                if r_idx > row:
                    new_edits[r_idx - 1] = ed
                else:
                    new_edits[r_idx] = ed
            self._edits = new_edits
        finally:
            self.endRemoveRows()

    def remove_row(self, row: int) -> None:
        """Compatibility wrapper for mark_row_deleted."""
        self.mark_row_deleted(row)

    def clear_pending_changes(self) -> None:
        """Clear tracked edits and deletions after they have been applied to the database."""
        self._edits.clear()
        self._deleted_pk_values.clear()

    def get_pk_values_for_row(self, row: int) -> Dict[str, Any]:
        """Return current PK values for the specified row index."""
        if not self._pk_columns:
            return {}
        try:
            return {pkc: self._rows[row][self._col_index[pkc]] for pkc in self._pk_columns}
        except Exception:
            return {}

    def get_columns(self) -> List[str]:
        return self._columns

    def get_pk_columns(self) -> List[str]:
        return list(self._pk_columns)