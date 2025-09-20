from PyQt6.QtWidgets import QWidget, QVBoxLayout, QPlainTextEdit, QTextEdit
from PyQt6.QtGui import QFont, QKeySequence, QShortcut
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QSyntaxHighlighter, QTextCharFormat, QColor, QTextFormat, QPalette
from PyQt6.QtCore import QRegularExpression
import sys
from PyQt6.QtWidgets import QPushButton, QHBoxLayout, QInputDialog, QLabel, QDialog, QDialogButtonBox, QPlainTextEdit
from PyQt6.QtCore import QThread
from PyQt6.QtGui import QTextCursor
from typing import Optional
import threading
import json
from PyQt6.QtGui import QPainter
from PyQt6.QtCore import QRect, QSize


class LineNumberArea(QWidget):
    def __init__(self, editor: 'CodeEditor'):
        super().__init__(editor)
        self._editor = editor

    def sizeHint(self) -> QSize:
        return QSize(self._editor.lineNumberAreaWidth(), 0)

    def paintEvent(self, event):
        self._editor.lineNumberAreaPaintEvent(event)


class CodeEditor(QPlainTextEdit):
    """QPlainTextEdit with a thin line-number gutter and subtle current-line highlight.

    Designed to blend with the iOS-like QSS theme (small gutter, light separators,
    soft current-line background).
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._line_number_area = LineNumberArea(self)
        self.blockCountChanged.connect(self.updateLineNumberAreaWidth)
        self.updateRequest.connect(self.updateLineNumberArea)
        self.cursorPositionChanged.connect(self.highlightCurrentLine)
        self.updateLineNumberAreaWidth(0)
        # Compute highlight colors from the current palette so dark mode works automatically
        pal = self.palette()
        try:
            hl = pal.color(QPalette.ColorRole.Highlight)
            # very subtle alpha blend of the highlight color
            self._current_line_color = QColor(hl.red(), hl.green(), hl.blue(), 22)
        except Exception:
            # fallback to a very faint blue
            self._current_line_color = QColor(10, 132, 255, 10)
        self.setViewportMargins(self.lineNumberAreaWidth(), 0, 0, 0)

    def lineNumberAreaWidth(self) -> int:
        # keep the gutter thin; allow up to 4 digits comfortably
        digits = max(2, len(str(max(1, self.blockCount()))))
        # base width + padding
        return 8 + digits * 8

    def updateLineNumberAreaWidth(self, _):
        self.setViewportMargins(self.lineNumberAreaWidth(), 0, 0, 0)

    def updateLineNumberArea(self, rect: QRect, dy: int) -> None:
        if dy:
            self._line_number_area.scroll(0, dy)
        else:
            self._line_number_area.update(0, rect.y(), self._line_number_area.width(), rect.height())

        if rect.contains(self.viewport().rect()):
            self.updateLineNumberAreaWidth(0)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        cr = self.contentsRect()
        self._line_number_area.setGeometry(QRect(cr.left(), cr.top(), self.lineNumberAreaWidth(), cr.height()))

    def lineNumberAreaPaintEvent(self, event) -> None:
        painter = QPainter(self._line_number_area)
        painter.fillRect(event.rect(), Qt.GlobalColor.transparent)

        block = self.firstVisibleBlock()
        block_number = block.blockNumber()
        top = int(self.blockBoundingGeometry(block).translated(self.contentOffset()).top())
        bottom = top + int(self.blockBoundingRect(block).height())

        # derive number color from palette to respect dark mode
        try:
            pal = self.palette()
            nc = pal.color(QPalette.ColorRole.Mid)
            # make sure it's slightly muted
            number_color = QColor(nc.red(), nc.green(), nc.blue(), 200)
        except Exception:
            number_color = QColor('#9aa8b3')
        painter.setPen(number_color)
        font = self.font()
        fm = self.fontMetrics()

        while block.isValid() and top <= event.rect().bottom():
            if block.isVisible() and bottom >= event.rect().top():
                number = str(block_number + 1)
                x = self._line_number_area.width() - fm.horizontalAdvance(number) - 6
                y = top + fm.ascent() + 2
                painter.drawText(x, y, number)
            block = block.next()
            top = bottom
            bottom = top + int(self.blockBoundingRect(block).height())
            block_number += 1

        # thin separator line to match QSS subtle divider (derived from palette)
        try:
            sep_c = self.palette().color(QPalette.ColorRole.Dark)
            sep_color = QColor(sep_c.red(), sep_c.green(), sep_c.blue(), 30)
        except Exception:
            sep_color = QColor(11, 26, 43, 12)
        painter.setPen(sep_color)
        painter.drawLine(self._line_number_area.width() - 1, event.rect().top(), self._line_number_area.width() - 1, event.rect().bottom())

    def highlightCurrentLine(self) -> None:
        # ExtraSelection is provided on QTextEdit, use that type even when inheriting QPlainTextEdit
        selection = QTextEdit.ExtraSelection()
        selection.format.setBackground(self._current_line_color)
        # Some PyQt6 builds do not expose QTextFormat.FullWidthSelection; avoid setting it to prevent AttributeError.
        # The background will still be applied to the selected line region (text width) which is visually acceptable.
        selection.cursor = self.textCursor()
        selection.cursor.clearSelection()
        self.setExtraSelections([selection])


# Background worker thread to call AI client without blocking the UI
class AIWorker(QThread):
    # Emit structured objects: progress emits tuples like (kind, text)
    result = pyqtSignal(str)
    error = pyqtSignal(str)
    progress = pyqtSignal(object)

    def __init__(self, prompt: str, parent=None, use_stream: bool = True):
        super().__init__(parent)
        self.prompt = prompt
        self.use_stream = use_stream
        self._stop_event = threading.Event()

    def stop(self):
        try:
            self._stop_event.set()
        except Exception:
            pass

    def _on_chunk(self, chunk):
        try:
            # forward structured chunk (kind, text) or raw string
            self.progress.emit(chunk)
        except Exception:
            pass

    def run(self):
        try:
            # Import here to avoid circular imports at module import time
            from utils.ai_client import generate_sql_from_nl

            if self.use_stream:
                # streaming callback will feed progress signals; pass stop_event for cooperative cancel
                res = generate_sql_from_nl(self.prompt, stream_callback=self._on_chunk, stop_event=self._stop_event)
            else:
                res = generate_sql_from_nl(self.prompt, stop_event=self._stop_event)

            if res is None:
                res = ""
            # Only emit result if not cancelled
            if not self._stop_event.is_set():
                self.result.emit(res)
        except Exception as e:
            # don't report errors if we were cancelled
            if not self._stop_event.is_set():
                self.error.emit(str(e))


class SqlEditor(QWidget):
    """A minimal SQL editor widget wrapping QPlainTextEdit.

    Emits execute_requested when the user presses Ctrl+Enter.
    Emits execute_selection_requested when the user presses Ctrl+Shift+Enter.
    """

    execute_requested = pyqtSignal()
    execute_selection_requested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        # small toolbar with AI button
        toolbar = QHBoxLayout()
        self.ai_btn = QPushButton("NL → SQL")
        self.ai_btn.setToolTip("Generate SQL from natural language using AI")
        self.ai_btn.clicked.connect(self._on_ai_button_clicked)
        toolbar.addWidget(self.ai_btn)

        # Add a SQL beautify/format button next to the AI button
        self.beautify_btn = QPushButton("Beautify")
        self.beautify_btn.setToolTip("Format/beautify SQL (uses sqlparse if installed)")
        self.beautify_btn.clicked.connect(self._on_beautify_clicked)
        toolbar.addWidget(self.beautify_btn)

        toolbar.addStretch()
        layout.addLayout(toolbar)

        self.editor = CodeEditor()
        self.editor.setLineWrapMode(QPlainTextEdit.LineWrapMode.WidgetWidth)
        # Choose a reasonable monospace font per platform
        if sys.platform.startswith("win"):
            family = "Consolas"
        elif sys.platform == "darwin":
            family = "Menlo"
        else:
            family = "Monospace"
        font = QFont(family)
        font.setPointSize(10)
        self.editor.setFont(font)
        layout.addWidget(self.editor)

        # Attach SQL highlighter to the editor's document
        self._highlighter = SqlHighlighter(self.editor.document())

        # Ctrl+Enter shortcut to request execution
        shortcut = QShortcut(QKeySequence("Ctrl+Return"), self.editor)
        shortcut.activated.connect(self.execute_requested)
        # Ctrl+Shift+Enter to execute only the current selection (if any)
        shortcut_sel = QShortcut(QKeySequence("Ctrl+Shift+Return"), self.editor)
        shortcut_sel.activated.connect(self.execute_selection_requested)

    def _on_ai_button_clicked(self):
        """Prompt the user for a natural-language request and call the AI in a background thread.

        Streaming chunks are shown in a small non-modal dialog so the main editor does
        not get repeatedly mutated during the generation. Only the final SQL is inserted
        into the main editor when generation completes.
        """
        try:
            # Prompt for NL input (large multi-line dialog)
            dlg = QDialog(self)
            dlg.setWindowTitle("Generate SQL from NL")
            dlg.resize(640, 240)
            dlg_layout = QVBoxLayout(dlg)
            lbl = QLabel("Describe the SQL you want:")
            dlg_layout.addWidget(lbl)
            nl_edit = QPlainTextEdit()
            nl_edit.setPlaceholderText("e.g. Show the top 10 customers by total spend in 2024")
            nl_edit.setFixedHeight(180)
            dlg_layout.addWidget(nl_edit)

            btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
            btn_box.accepted.connect(dlg.accept)
            btn_box.rejected.connect(dlg.reject)
            dlg_layout.addWidget(btn_box)

            if dlg.exec() != QDialog.DialogCode.Accepted:
                return

            text = nl_edit.toPlainText()
            if not text or not text.strip():
                return

            # disable button while working and show inline preview dialog for streaming
            self.ai_btn.setEnabled(False)
            self.setCursor(Qt.CursorShape.WaitCursor)

            # Create a non-modal preview dialog with split panes for reasoning vs content
            preview = QDialog(self)
            preview.setWindowTitle("AI generation preview")
            preview.resize(900, 500)
            pv_layout = QVBoxLayout(preview)
            pv_top = QHBoxLayout()
            pv_layout.addLayout(pv_top)
            pv_label = QLabel("AI is thinking... (streaming)")
            pv_top.addWidget(pv_label)
            cancel_btn = QPushButton("Cancel")
            pv_top.addWidget(cancel_btn)
            # Add a Close button so the user can keep the preview open to inspect the process;
            # it is disabled while the worker is active and enabled when finished or errored.
            close_btn = QPushButton("Close")
            close_btn.setEnabled(False)
            pv_top.addWidget(close_btn)
            # Allow user to close the preview when enabled
            try:
                close_btn.clicked.connect(preview.close)
            except Exception:
                pass

            # Usage/info panel (shows model usage tokens etc.)
            usage_edit = QPlainTextEdit()
            usage_edit.setReadOnly(True)
            usage_edit.setPlaceholderText("Usage / metadata from AI (e.g. token usage)")
            usage_edit.setFixedHeight(90)
            usage_edit.setFont(self.editor.font())
            pv_layout.addWidget(usage_edit)

            # two side-by-side read-only editors: left = reasoning, right = final content
            split_layout = QHBoxLayout()
            preview.setLayout(pv_layout)
            reasoning_edit = QPlainTextEdit()
            reasoning_edit.setReadOnly(True)
            reasoning_edit.setPlaceholderText("Reasoning / chain-of-thought")
            reasoning_edit.setFont(self.editor.font())
            content_edit = QPlainTextEdit()
            content_edit.setReadOnly(True)
            content_edit.setPlaceholderText("Final content / SQL")
            content_edit.setFont(self.editor.font())
            split_layout.addWidget(reasoning_edit)
            split_layout.addWidget(content_edit)
            pv_layout.addLayout(split_layout)
            preview.show()

            # Buffer to collect only content chunks (final SQL pieces). Reasoning is kept only in preview.
            content_chunks = []

            def _on_progress(chunk):
                try:
                    if not chunk:
                        return
                    # chunk is expected to be a tuple (kind, text) per ai_client changes
                    try:
                        if isinstance(chunk, tuple) and len(chunk) == 2:
                            kind, txt = chunk
                        elif isinstance(chunk, list) and len(chunk) == 2:
                            kind, txt = chunk
                        else:
                            kind, txt = ("content", str(chunk))
                    except Exception:
                        kind, txt = ("content", str(chunk))

                    if kind == "reasoning":
                        reasoning_edit.moveCursor(QTextCursor.MoveOperation.End)
                        reasoning_edit.insertPlainText(str(txt))
                        reasoning_edit.moveCursor(QTextCursor.MoveOperation.End)
                    elif kind == "content":
                        # show content in the preview content pane and also collect it for final insertion
                        content_edit.moveCursor(QTextCursor.MoveOperation.End)
                        content_edit.insertPlainText(str(txt))
                        content_edit.moveCursor(QTextCursor.MoveOperation.End)
                        try:
                            content_chunks.append(str(txt))
                        except Exception:
                            pass
                    elif kind == "usage":
                        # display usage/metadata in the usage panel
                        try:
                            if isinstance(txt, (dict, list)):
                                usage_text = json.dumps(txt, indent=2)
                            else:
                                usage_text = str(txt)
                            usage_edit.setPlainText(usage_text)
                        except Exception:
                            try:
                                usage_edit.setPlainText(str(txt))
                            except Exception:
                                pass
                    else:
                        # preview or unknown kinds go to reasoning pane
                        reasoning_edit.moveCursor(QTextCursor.MoveOperation.End)
                        reasoning_edit.insertPlainText(f"[{kind}] " + str(txt))
                        reasoning_edit.moveCursor(QTextCursor.MoveOperation.End)
                except Exception:
                    pass

            cancelled = {'v': False}

            def _on_result(res: str):
                try:
                    # if the worker was cancelled, do not insert final SQL
                    if cancelled['v']:
                        return
                    # Prefer the assembled content chunks (streaming content) as the final SQL so reasoning is not included.
                    final = ''.join(content_chunks).strip()
                    # Fallback to the worker's returned text if no content chunks were received (non-streaming or unexpected format)
                    if not final:
                        final = (res or "").strip()
                    # Insert final SQL into the main editor at current cursor position
                    try:
                        cur = self.editor.textCursor()
                        cur.insertText(final + "\n")
                        self.editor.setFocus()
                    except Exception:
                        # fallback: append at end
                        c = self.editor.textCursor()
                        c.movePosition(QTextCursor.MoveOperation.End)
                        c.insertText(final + "\n")
                except Exception:
                    pass
                finally:
                    # Keep the preview dialog open so the user can inspect the streamed content.
                    # Update the status label and enable the Close button so the user may close it.
                    try:
                        pv_label.setText("AI generation finished")
                    except Exception:
                        pass
                    try:
                        close_btn.setEnabled(True)
                    except Exception:
                        pass
                    self.ai_btn.setEnabled(True)
                    self.unsetCursor()

            def _on_error(msg: str):
                try:
                    from PyQt6.QtWidgets import QMessageBox
                    QMessageBox.critical(self, "AI Error", f"Failed to generate SQL: {msg}")
                except Exception:
                    pass
                finally:
                    # Don't auto-close the preview; show an error state and enable Close so user can inspect.
                    try:
                        pv_label.setText("AI generation failed")
                    except Exception:
                        pass
                    try:
                        close_btn.setEnabled(True)
                    except Exception:
                        pass
                    self.ai_btn.setEnabled(True)
                    self.unsetCursor()

            def _on_cancel():
                # mark cancelled and request worker stop
                try:
                    cancelled['v'] = True
                    if getattr(self, '_ai_worker', None):
                        try:
                            self._ai_worker.stop()
                        except Exception:
                            pass
                except Exception:
                    pass
                finally:
                    # If the user explicitly cancels, close the preview immediately.
                    try:
                        preview.close()
                    except Exception:
                        pass
                    # UI widgets (like ai_btn) might have been deleted; guard access
                    try:
                        if getattr(self, 'ai_btn', None):
                            self.ai_btn.setEnabled(True)
                    except Exception:
                        # widget deleted or other error; ignore
                        pass
                    try:
                        self.unsetCursor()
                    except Exception:
                        pass

            cancel_btn.clicked.connect(_on_cancel)
            # ensure closing the preview dialog via window close also cancels the worker
            try:
                preview.rejected.connect(_on_cancel)
            except Exception:
                pass
            try:
                preview.destroyed.connect(lambda _: _on_cancel())
            except Exception:
                pass

            self._ai_worker = AIWorker(text.strip(), parent=self, use_stream=True)
            self._ai_worker.progress.connect(_on_progress)
            self._ai_worker.result.connect(_on_result)
            self._ai_worker.error.connect(_on_error)
            # ensure worker cleanup
            self._ai_worker.finished.connect(lambda: (setattr(self, "_ai_worker", None), None))
            self._ai_worker.start()
        except Exception as e:
            try:
                from PyQt6.QtWidgets import QMessageBox

                QMessageBox.critical(self, "AI Error", f"Unexpected error: {e}")
            except Exception:
                pass
            finally:
                self.ai_btn.setEnabled(True)
                self.unsetCursor()

    def _on_beautify_clicked(self) -> None:
        """Format the selected SQL or the whole editor content.

        Prefers the 'sqlparse' library if available; otherwise falls back to a simple
        whitespace-normalization fallback. Any errors are shown to the user.
        """
        try:
            cursor = self.editor.textCursor()
            selected = cursor.selectedText()
            if selected:
                # QPlainTextEdit returns U+2029 for newlines in selectedText
                sql = selected.replace("\u2029", "\n")
                target_selection = True
            else:
                sql = self.editor.toPlainText()
                target_selection = False

            if not sql or not sql.strip():
                return

            formatted = None
            try:
                # Import locally so sqlparse is optional
                import sqlparse  # type: ignore

                formatted = sqlparse.format(sql, reindent=True, keyword_case='upper')
            except Exception:
                # fallback: basic normalization (collapse excessive whitespace, keep simple semicolon separation)
                parts = [p.strip() for p in sql.split(';') if p.strip()]
                if parts:
                    formatted = ';\n'.join(parts)
                    if sql.strip().endswith(';'):
                        formatted += ';'
                else:
                    formatted = ' '.join(sql.split())

            if formatted is None:
                formatted = sql

            # Replace selection or whole document preserving focus/cursor
            if target_selection:
                # replace current selection
                cursor.insertText(formatted)
            else:
                # try preserve scroll/position by replacing document and restoring cursor
                cur = self.editor.textCursor()
                pos = cur.position()
                self.editor.setPlainText(formatted)
                # restore a sensible cursor position
                new_cursor = self.editor.textCursor()
                new_cursor.setPosition(min(pos, len(formatted)))
                self.editor.setTextCursor(new_cursor)
                self.editor.setFocus()
        except Exception as e:
            try:
                from PyQt6.QtWidgets import QMessageBox

                QMessageBox.critical(self, "Format Error", f"Failed to format SQL: {e}")
            except Exception:
                pass

    def get_sql(self) -> str:
        """Return selected SQL if present, otherwise whole text."""
        cursor = self.editor.textCursor()
        selected = cursor.selectedText()
        if selected:
            # QPlainTextEdit returns newlines as unicode U+2029 in selectedText; replace with \n
            return selected.replace("\u2029", "\n").strip()
        return self.editor.toPlainText().strip()

    def get_selected_sql(self) -> str:
        """Return only the currently selected text (normalized), or empty string if none."""
        cursor = self.editor.textCursor()
        selected = cursor.selectedText()
        if not selected:
            return ""
        return selected.replace("\u2029", "\n").strip()

    def set_sql(self, sql: str):
        self.editor.setPlainText(sql)

    def clear(self):
        self.editor.clear()


class SqlHighlighter(QSyntaxHighlighter):
    """Basic SQL syntax highlighter for QPlainTextEdit/QTextDocument.

    Highlights common SQL keywords, functions, strings, numbers and comments.
    This is intentionally lightweight and can be extended later (e.g. schema-aware
    completion / highlighting).
    """

    def __init__(self, document):
        super().__init__(document)
        # Prepare formats derived from the application's palette so QSS-driven
        # theme changes (dark/light) influence syntax colors.
        from PyQt6.QtWidgets import QApplication

        pal = None
        try:
            app = QApplication.instance()
            pal = app.palette() if app is not None else QPalette()
        except Exception:
            pal = QPalette()

        base = pal.color(QPalette.ColorRole.Text)
        highlight = pal.color(QPalette.ColorRole.Highlight)
        mid = pal.color(QPalette.ColorRole.Mid)

        def safe_lighter(col, factor=120):
            try:
                return col.lighter(factor)
            except Exception:
                return col

        def safe_darker(col, factor=120):
            try:
                return col.darker(factor)
            except Exception:
                return col

        # Keywords use a tinted variant of the highlight color
        self.keyword_format = QTextCharFormat()
        self.keyword_format.setForeground(safe_lighter(highlight, 125))
        self.keyword_format.setFontWeight(QFont.Weight.Bold)

        # Operators: slightly muted compared to base text
        self.operator_format = QTextCharFormat()
        self.operator_format.setForeground(safe_darker(mid if mid.isValid() else base, 115))

        # Strings: use a subtle greenish tint derived from base
        self.string_format = QTextCharFormat()
        try:
            s = safe_lighter(base, 110)
            s.setHsv((s.hue() or 120), min(255, int(s.saturation() * 1.1)), s.value())
            self.string_format.setForeground(s)
        except Exception:
            self.string_format.setForeground(safe_lighter(base, 110))

        # Numbers: use a variant of highlight
        self.number_format = QTextCharFormat()
        self.number_format.setForeground(safe_darker(highlight, 120))

        # Comments: use placeholder/mid color and italicize
        self.comment_format = QTextCharFormat()
        try:
            placeholder = pal.color(QPalette.ColorRole.PlaceholderText)
            if placeholder.isValid():
                self.comment_format.setForeground(placeholder)
            else:
                self.comment_format.setForeground(safe_lighter(mid, 130))
        except Exception:
            self.comment_format.setForeground(safe_lighter(mid, 130))
        self.comment_format.setFontItalic(True)

        # Functions: slightly brighter than highlight
        self.function_format = QTextCharFormat()
        self.function_format.setForeground(safe_lighter(highlight, 140))

        # Build regex rules
        keywords = (
            "SELECT|FROM|WHERE|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|DROP|ALTER|ADD|COLUMN|INDEX|VIEW|TRIGGER|PRIMARY|KEY|FOREIGN|REFERENCES|CONSTRAINT|UNIQUE|NOT|NULL|DEFAULT|CHECK|JOIN|INNER|LEFT|RIGHT|FULL|OUTER|ON|USING|GROUP|BY|ORDER|HAVING|LIMIT|OFFSET|DISTINCT|AS|UNION|ALL|EXISTS|BETWEEN|LIKE|IN|CASE|WHEN|THEN|ELSE|END"
        )

        functions = (
            "COUNT|SUM|AVG|MIN|MAX|NOW|COALESCE|NULLIF|IFNULL|LENGTH|SUBSTR"
        )

        operators = r"[=<>!~\+\-\*/%]+"

        # compile rules as (QRegularExpression, QTextCharFormat)
        self.rules = []
        # keywords
        # Use inline case-insensitive flag (?i) to avoid binding differences in PyQt6's enum names
        kw_pattern = QRegularExpression(r"(?i)\b(" + keywords + r")\b")
        self.rules.append((kw_pattern, self.keyword_format))
        # functions (name followed by open paren)
        fn_pattern = QRegularExpression(r"\b(" + functions + r")\b\s*(?=\()")
        self.rules.append((fn_pattern, self.function_format))
        # operators
        op_pattern = QRegularExpression(operators)
        self.rules.append((op_pattern, self.operator_format))
        # numeric literals
        num_pattern = QRegularExpression(r"\b\d+(?:\.\d+)?\b")
        self.rules.append((num_pattern, self.number_format))
        # single-quoted and double-quoted strings
        str_pattern = QRegularExpression(r"'(?:''|[^'])*'|\"(?:\\\"|[^\"])*\"")
        self.rules.append((str_pattern, self.string_format))
        # single-line comments -- ...
        self.comment_start = QRegularExpression(r"--.*")
        # multi-line comment delimiters
        self.comment_start_delim = QRegularExpression(r"/\*")
        self.comment_end_delim = QRegularExpression(r"\*/")

    def highlightBlock(self, text: str) -> None:
        """Apply syntax highlighting rules to the given block of text.

        Supports multi-line C-style comments with block state tracking.
        """
        # apply standard rules
        for pattern, fmt in getattr(self, 'rules', []):
            it = pattern.globalMatch(text)
            while it.hasNext():
                match = it.next()
                start = match.capturedStart()
                length = match.capturedLength()
                self.setFormat(start, length, fmt)

        # single-line comments
        itc = self.comment_start.globalMatch(text)
        while itc.hasNext():
            m = itc.next()
            self.setFormat(m.capturedStart(), m.capturedLength(), self.comment_format)

        # handle multi-line comments with block state
        start_idx = 0
        if self.previousBlockState() == 1:
            start_idx = 0
        else:
            m = self.comment_start_delim.match(text)
            start_idx = m.capturedStart() if m.hasMatch() else -1

        while start_idx >= 0:
            m_end = self.comment_end_delim.match(text, start_idx)
            if m_end.hasMatch():
                end_idx = m_end.capturedEnd()
                comment_len = end_idx - start_idx
                self.setFormat(start_idx, comment_len, self.comment_format)
                start_idx = -1
            else:
                # comment continues to next block
                self.setFormat(start_idx, len(text) - start_idx, self.comment_format)
                self.setCurrentBlockState(1)
                start_idx = -1

        # if no unclosed comment found, reset block state
        if self.currentBlockState() != 1:
            self.setCurrentBlockState(0)