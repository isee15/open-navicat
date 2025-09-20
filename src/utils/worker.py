from PyQt6.QtCore import QThread, pyqtSignal
from typing import List, Tuple, Any
import threading

class ExecutionWorker(QThread):
    """Run SQL execution in a background thread and emit results.

    Uses threading.Event to support cancellation; execute_sql is called in this thread.
    """

    results_ready = pyqtSignal(object)  # will emit List[Tuple[List[str], List[Tuple]]]
    error = pyqtSignal(str)
    finished_signal = pyqtSignal()

    def __init__(self, engine, sql: str, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.sql = sql
        self._stop_event = threading.Event()

    def run(self):
        # delay import to avoid circular
        from db.executor import execute_sql

        try:
            results = execute_sql(self.engine, self.sql, stop_event=self._stop_event)
            self.results_ready.emit(results)
        except Exception as e:
            self.error.emit(str(e))
        finally:
            self.finished_signal.emit()

    def stop(self):
        self._stop_event.set()